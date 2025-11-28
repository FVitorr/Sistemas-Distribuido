package server;

import org.jgroups.*;
import org.jgroups.util.Util;

import java.io.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * ControleServer - Gerencia o cluster com JGroups (Distribuição, Lock, Estado, Hash)
 */

public class ControleServer implements ControleService, Receiver, Closeable {

    private static final String CLUSTER = "FileServerCluster";
    private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

    protected JChannel canal;
    protected DadosServer dados;

    private final Map<String, Long> metadata = new ConcurrentHashMap<>();

    // Locks distribuídos
    private final Map<String, Boolean> arquivosBloqueados = new ConcurrentHashMap<>();
    private final Map<String, Queue<Address>> filasDeLock = new ConcurrentHashMap<>();

    protected Address lider;

    public ControleServer(DadosServer dados) throws Exception {
        this.dados = dados;

        canal = new JChannel("jgroups.xml");
        canal.setReceiver(this);
        canal.connect(CLUSTER);

        log("SERVIDOR CONECTADO ao cluster como: " + canal.getAddress());

        atualizarMetadataLocal();
    }

    public ControleServer() throws Exception {
        this(new DadosServer());
    }

    // -------------------------------------------------------------------------
    //  LOG
    // -------------------------------------------------------------------------
    private void log(String msg) {
        System.out.println("[" + sdf.format(new Date()) + "] " + msg);
    }

    // -------------------------------------------------------------------------
    //  API RMI chamada pelo Gateway
    // -------------------------------------------------------------------------

    public List<String> listarArquivos() {
        log("Listagem enviada ao cliente.");
        return dados.listarArquivos();
    }

    public synchronized boolean upload(String nome, byte[] conteudo) {
        try {
            log("Cliente solicitou UPLOAD: " + nome + " (" + conteudo.length + " bytes)");

            adquirirLock(nome);

            boolean ok = dados.salvarArquivo(nome, conteudo);
            if (!ok) return false;

            metadata.put(nome, (long) conteudo.length);

            MensagemCluster msg = MensagemCluster.upload(nome, conteudo);
            canal.send(new ObjectMessage(null, msg));

            log("UPLOAD concluído e replicado no cluster: " + nome);
            return true;

        } catch (Exception e) {
            log("ERRO NO UPLOAD: " + e.getMessage());
            return false;

        } finally {
            liberarLock(nome);
        }
    }

    public byte[] download(String nome) {
        log("Cliente requisitou DOWNLOAD: " + nome);
        return dados.lerArquivo(nome);
    }

    public String gerarHashGlobal() {
        log("Gerando HASH GLOBAL do sistema...");

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

    // -------------------------------------------------------------------------
    //  CALLBACKS JGROUPS
    // -------------------------------------------------------------------------

    @Override
    public void receive(Message msg) {
        Object obj = msg.getObject();
        if (!(obj instanceof MensagemCluster)) return;

        MensagemCluster m = (MensagemCluster) obj;

        switch (m.acao) {
            case UPLOAD -> aplicarUploadCluster(m);
            case LOCK_REQUEST -> processarPedidoDeLock(msg.getSrc(), m.arquivo);
            case LOCK_RELEASE -> processarLiberacaoDeLock(m.arquivo);
        }
    }

    @Override
    public void viewAccepted(View view) {
        log("NOVA VIEW DO CLUSTER: " + view);

        Address novoLider = view.getCreator();
        if (lider == null || !lider.equals(novoLider)) {
            log("NOVO LÍDER ELEITO: " + novoLider);
        }

        lider = novoLider;

        log("SERVIDORES ATIVOS NO CLUSTER:");
        for (Address a : view.getMembers()) {
            log(" - " + a);
        }
    }

    // Estado do cluster
    @Override
    public void getState(OutputStream out) throws Exception {
        log("Enviando estado local para novo nó...");
        synchronized (metadata) {
            Util.objectToStream(metadata, new DataOutputStream(out));
        }
    }

    @Override
    public void setState(InputStream in) throws Exception {
        Map<String, Long> m =
                (Map<String, Long>) Util.objectFromStream(new DataInputStream(in));

        metadata.clear();
        metadata.putAll(m);

        log("ESTADO RESTAURADO: " + metadata.size() + " arquivos.");
    }

    // -------------------------------------------------------------------------
    //  REPLICAÇÃO
    // -------------------------------------------------------------------------

    private void aplicarUploadCluster(MensagemCluster m) {
        synchronized (this) {
            log("RECEBIDO UPLOAD REMOTO do cluster: " + m.arquivo);
            dados.salvarArquivo(m.arquivo, m.conteudo);
            metadata.put(m.arquivo, (long) m.conteudo.length);
        }
    }

    // -------------------------------------------------------------------------
    //  LOCK DISTRIBUÍDO
    // -------------------------------------------------------------------------

    private void adquirirLock(String arquivo) throws Exception {
        if (souLider()) {
            log("Sou LÍDER. Bloqueando localmente: " + arquivo);
            bloquearOuEnfileirar(arquivo, canal.getAddress());
            return;
        }

        log("Solicitando LOCK ao líder para arquivo: " + arquivo);

        MensagemCluster req = MensagemCluster.solicitarLock(arquivo);
        canal.send(new ObjectMessage(lider, req));

        while (!possoUsar(arquivo)) {
            Thread.sleep(30);
        }

        log("LOCK CONCEDIDO: " + arquivo);
    }

    private void liberarLock(String arquivo) {
        try {
            MensagemCluster m = MensagemCluster.liberarLock(arquivo);
            canal.send(new ObjectMessage(null, m));
            log("LOCK LIBERADO: " + arquivo);
        } catch (Exception ignored) {
        }
    }

    private void processarPedidoDeLock(Address origem, String arquivo) {
        if (!souLider()) return;

        log("LÍDER: pedido de lock de " + origem + " para arquivo " + arquivo);
        bloquearOuEnfileirar(arquivo, origem);
    }

    private void processarLiberacaoDeLock(String arquivo) {
        if (!souLider()) return;

        log("LÍDER: liberando lock de " + arquivo);

        Queue<Address> fila = filasDeLock.computeIfAbsent(arquivo, f -> new ArrayDeque<>());
        fila.poll();

        if (fila.isEmpty()) {
            arquivosBloqueados.put(arquivo, false);
            log("Arquivo liberado completamente: " + arquivo);
        }
    }

    private void bloquearOuEnfileirar(String arquivo, Address solicitante) {
        boolean bloqueado = arquivosBloqueados.getOrDefault(arquivo, false);

        Queue<Address> fila = filasDeLock.computeIfAbsent(arquivo, f -> new ArrayDeque<>());

        if (!bloqueado) {
            arquivosBloqueados.put(arquivo, true);
            fila.add(solicitante);
            log("LOCK atribuído imediatamente para " + solicitante);
            return;
        }

        fila.add(solicitante);
        log("LOCK ocupado — adicionando à fila: " + solicitante);
    }

    private boolean possoUsar(String arquivo) {
        Queue<Address> fila = filasDeLock.computeIfAbsent(arquivo, f -> new ArrayDeque<>());
        return !fila.isEmpty() && fila.peek().equals(canal.getAddress());
    }

    private boolean souLider() {
        return canal.getAddress().equals(lider);
    }

    // -------------------------------------------------------------------------
    //  UTILITÁRIOS
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
        log("Servidor sendo fechado...");
        canal.close();
    }

    public static void main(String[] args) {
        try {
            ControleServer server = new ControleServer(); // instancia o servidor de arquivos

            // Exporta o ControleServer como objeto RMI
            ControleService stub = (ControleService) UnicastRemoteObject.exportObject(server, 0);

            // Cria o registry na porta 1098 e registra o stub
            Registry registry = LocateRegistry.createRegistry(1098);
            registry.rebind("ControleService", stub);

            System.out.println("ControleServer iniciado na porta 1098!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
