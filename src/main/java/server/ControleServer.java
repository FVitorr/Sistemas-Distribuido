package server;

import model.Usuario;
import org.jgroups.*;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Util;

import java.io.*;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ControleServer implements Receiver, Closeable {

    private static final String CLUSTER = "FileServerCluster";
    private static final String RPC_CLUSTER = "FileServerRPC";
    private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

    // Canal do cluster (replicação, locks)
    protected JChannel canalCluster;

    // Canal RPC (exposto ao Gateway)
    protected JChannel canalRPC;
    protected RpcDispatcher dispatcher;

    protected DadosServer dados;

    private final Map<String, Long> metadata = new ConcurrentHashMap<>();
    private final Map<String, Boolean> arquivosBloqueados = new ConcurrentHashMap<>();
    private final Map<String, Queue<Address>> filasDeLock = new ConcurrentHashMap<>();

    protected Address lider;

    public ControleServer(DadosServer dados) throws Exception {
        this.dados = dados;

        // 1) Conectar no cluster real
        canalCluster = new JChannel("jgroups.xml");
        canalCluster.setReceiver(this);
        canalCluster.connect(CLUSTER);

        // 2) Canal RPC (para Gateway chamar métodos via RpcDispatcher)
        canalRPC = new JChannel("jgroups.xml");
        canalRPC.connect(RPC_CLUSTER);

        // dispatcher ligado no canalRPC (server responde RPCs aqui)
        dispatcher = new RpcDispatcher(canalRPC, this);
        dispatcher.setReceiver(this);

        log("SERVIDOR conectado ao cluster: " + canalCluster.getAddress());
        log("SERVIDOR RPC ativo em: " + canalRPC.getAddress());

        // opcional: anunciar meu endereço RPC via canalCluster (ajuda gateways que só leem view)
        MensagemCluster reg = MensagemCluster.registerRpc(canalRPC.getAddress().toString());
        canalCluster.send(new ObjectMessage(null, reg));

        atualizarMetadataLocal();
    }

    public ControleServer() throws Exception {
        this(new DadosServer());
    }

    private void log(String msg) {
        System.out.println("[" + sdf.format(new Date()) + "] " + msg);
    }

    // -------------------------------------------------------------------------
    // RPC methods (Gateway chama via canalRPC -> dispatcher)
    // -------------------------------------------------------------------------
    public boolean isBackend() { return true; }

    public boolean login(String username, String password) {
        log("🔍 LOGIN solicitado (RPC) no servidor " + canalRPC.getAddress() + ": " + username);
        return dados.validarUsuario(username, password);
    }

    public List<String> listarArquivos() {
        log("LISTAR ARQUIVOS solicitado (RPC)");
        return dados.listarArquivos();
    }

    public synchronized boolean upload(String nome, byte[] conteudo) {
        try {
            log("UPLOAD solicitado (RPC): " + nome + " (" + conteudo.length + " bytes)");
            adquirirLock(nome);

            boolean ok = dados.salvarArquivo(nome, conteudo);
            if (!ok) return false;

            metadata.put(nome, (long) conteudo.length);

            // Replica para outros servidores via canalCluster
            MensagemCluster msg = MensagemCluster.upload(nome, conteudo);
            canalCluster.send(new ObjectMessage(null, msg));

            log("UPLOAD concluído e replicado: " + nome);
            return true;

        } catch (Exception e) {
            log("ERRO NO UPLOAD: " + e.getMessage());
            return false;
        } finally {
            liberarLock(nome);
        }
    }

    public byte[] download(String nome) {
        log("DOWNLOAD solicitado (RPC): " + nome);
        return dados.lerArquivo(nome);
    }

    public String gerarHashGlobal() {
        log("HASH GLOBAL solicitado (RPC)");
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            List<String> arquivos = listarArquivos();
            Collections.sort(arquivos);
            for (String f : arquivos) {
                byte[] content = download(f);
                if (content != null) md.update(content);
            }
            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) sb.append(String.format("%02x", b));
            String hash = sb.toString();
            log("HASH GLOBAL = " + hash);
            return hash;
        } catch (Exception e) {
            return "ERRO-HASH";
        }
    }

    public synchronized boolean salvarUsuario(Usuario usuario) {
        try {
            log("SALVAR USUÁRIO solicitado (RPC): " + usuario.getUsername());
            boolean ok = dados.salvarUsuario(usuario);
            if (!ok) {
                log("ERRO ao salvar usuário localmente");
                return false;
            }
            MensagemCluster msg = MensagemCluster.salvarUsuario(usuario);
            canalCluster.send(new ObjectMessage(null, msg));
            log("USUÁRIO salvo e replicado: " + usuario.getUsername());
            return true;
        } catch (Exception e) {
            log("ERRO AO SALVAR USUÁRIO: " + e.getMessage());
            return false;
        }
    }

    // -------------------------------------------------------------------------
    // Callbacks do cluster (replicação, locks, view)
    // -------------------------------------------------------------------------
    @Override
    public void receive(Message msg) {
        // evita aplicar mensagens que eu mesmo enviei via canalCluster
        if (msg.getSrc() != null && msg.getSrc().equals(canalCluster.getAddress())) {
            return;
        }

        Object obj = msg.getObject();
        if (!(obj instanceof MensagemCluster)) return;
        MensagemCluster m = (MensagemCluster) obj;

        switch (m.acao) {
            case UPLOAD -> aplicarUploadCluster(m);
            case LOCK_REQUEST -> processarPedidoDeLock(msg.getSrc(), m.arquivo);
            case LOCK_RELEASE -> processarLiberacaoDeLock(m.arquivo);
            case SALVAR_USUARIO -> aplicarSalvarUsuarioCluster(m);
            case REGISTER_RPC_ADDRESS -> {
                // outro servidor anunciou seu endereço RPC
                log("Recebido registro RPC de " + m.rpcAddress);
            }
        }
    }

    @Override
    public void viewAccepted(View view) {
        log("═══════════════════════════════════════");
        log("NOVA VIEW DO CLUSTER: " + view);
        Address novoLider = view.getCreator();
        boolean liderMudou = (lider == null || !lider.equals(novoLider));
        lider = novoLider;
        if (liderMudou) log(">>> LÍDER DO CLUSTER: " + lider);
        log("MEMBROS ATIVOS NO CLUSTER:");
        for (int i = 0; i < view.getMembers().size(); i++) {
            Address addr = view.getMembers().get(i);
            String tipo = addr.equals(lider) ? " [LÍDER]" : "";
            String eu = addr.equals(canalCluster.getAddress()) ? " [EU]" : "";
            log("  [" + i + "] " + addr + tipo + eu);
        }
        log("═══════════════════════════════════════");
    }

    @Override
    public void getState(OutputStream out) throws Exception {
        log("Enviando estado para novo membro...");
        synchronized (metadata) {
            Util.objectToStream(metadata, new DataOutputStream(out));
        }
    }

    @Override
    public void setState(InputStream in) throws Exception {
        log("Recebendo estado do cluster...");
        Map<String, Long> m = (Map<String, Long>) Util.objectFromStream(new DataInputStream(in));
        metadata.clear();
        metadata.putAll(m);
        log("ESTADO RESTAURADO: " + metadata.size() + " arquivos.");
    }

    // -------------------------------------------------------------------------
    // REPLICAÇÃO handlers
    // -------------------------------------------------------------------------
    private void aplicarUploadCluster(MensagemCluster m) {
        synchronized (this) {
            log("RECEBENDO replicação UPLOAD: " + m.arquivo);
            dados.salvarArquivo(m.arquivo, m.conteudo);
            metadata.put(m.arquivo, (long) m.conteudo.length);
        }
    }

    private void aplicarSalvarUsuarioCluster(MensagemCluster m) {
        synchronized (this) {
            log("RECEBENDO replicação USUÁRIO: " + m.usuario.getUsername());
            dados.salvarUsuario(m.usuario);
        }
    }

    // -------------------------------------------------------------------------
    // LOCK DISTRIBUÍDO (usando canalCluster)
    // -------------------------------------------------------------------------
    private void adquirirLock(String arquivo) throws Exception {
        if (souLider()) {
            log("Sou LÍDER. Bloqueando localmente: " + arquivo);
            bloquearOuEnfileirar(arquivo, canalCluster.getAddress());
            return;
        }
        log("Solicitando LOCK ao líder: " + arquivo);
        MensagemCluster req = MensagemCluster.solicitarLock(arquivo);
        canalCluster.send(new ObjectMessage(lider, req));
        while (!possoUsar(arquivo)) Thread.sleep(30);
        log("LOCK CONCEDIDO: " + arquivo);
    }

    private void liberarLock(String arquivo) {
        try {
            MensagemCluster m = MensagemCluster.liberarLock(arquivo);
            canalCluster.send(new ObjectMessage(null, m));
            log("LOCK LIBERADO: " + arquivo);
        } catch (Exception ignored) {}
    }

    private void processarPedidoDeLock(Address origem, String arquivo) {
        if (!souLider()) return;
        log("LÍDER processando lock de " + origem + " para " + arquivo);
        bloquearOuEnfileirar(arquivo, origem);
    }

    private void processarLiberacaoDeLock(String arquivo) {
        if (!souLider()) return;
        Queue<Address> fila = filasDeLock.computeIfAbsent(arquivo, f -> new ArrayDeque<>());
        fila.poll();
        if (fila.isEmpty()) {
            arquivosBloqueados.put(arquivo, false);
            log("Arquivo completamente liberado: " + arquivo);
        }
    }

    private void bloquearOuEnfileirar(String arquivo, Address solicitante) {
        boolean bloqueado = arquivosBloqueados.getOrDefault(arquivo, false);
        Queue<Address> fila = filasDeLock.computeIfAbsent(arquivo, f -> new ArrayDeque<>());
        if (!bloqueado) {
            arquivosBloqueados.put(arquivo, true);
            fila.add(solicitante);
            log("LOCK concedido imediatamente para " + solicitante);
            return;
        }
        fila.add(solicitante);
        log("LOCK ocupado - enfileirando: " + solicitante);
    }

    private boolean possoUsar(String arquivo) {
        Queue<Address> fila = filasDeLock.computeIfAbsent(arquivo, f -> new ArrayDeque<>());
        return !fila.isEmpty() && fila.peek().equals(canalCluster.getAddress());
    }

    private boolean souLider() {
        return canalCluster.getAddress().equals(lider);
    }

    // -------------------------------------------------------------------------
    // UTILITÁRIOS
    // -------------------------------------------------------------------------
    private void atualizarMetadataLocal() {
        for (String f : dados.listarArquivos()) {
            byte[] content = dados.lerArquivo(f);
            metadata.put(f, content == null ? 0L : (long) content.length);
        }
        log("Metadata local carregada: " + metadata.size() + " arquivos.");
    }

    @Override
    public void close() {
        log("Servidor sendo encerrado...");
        if (dispatcher != null) dispatcher.stop();
        if (canalRPC != null) canalRPC.close();
        if (canalCluster != null) canalCluster.close();
    }

    // -------------------------------------------------------------------------
    // MAIN
    // -------------------------------------------------------------------------
    public static void main(String[] args) {
        try {
            System.out.println("╔════════════════════════════════════════════╗");
            System.out.println("║     CONTROLE SERVER (JGroups Only)         ║");
            System.out.println("╚════════════════════════════════════════════╝");

            ControleServer server = new ControleServer();

            System.out.println("✓ Servidor conectado ao cluster e RPC");
            Thread.currentThread().join();

        } catch (Exception e) {
            System.err.println("ERRO ao iniciar servidor:");
            e.printStackTrace();
        }
    }
}
