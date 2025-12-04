package server;

import model.Usuario;
import org.jgroups.*;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.blocks.cs.ReceiverAdapter;
import org.jgroups.util.Util;
import security.JwtUtil;

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

    public static class EstadoCluster implements Serializable {
        public Map<String, Long> metadata;
        public Map<String, byte[]> arquivos;
        public List<Usuario> usuarios;
    }


    public ControleServer(DadosServer dados) throws Exception {
        this.dados = new DadosServer();  // único para este servidor

        // 1) Conectar no cluster real
        canalCluster = new JChannel("jgroups.xml");
        canalCluster.setReceiver(this);
        canalCluster.connect(CLUSTER);

        // 2) Canal RPC (para Gateway chamar métodos via RpcDispatcher)
        canalRPC = new JChannel("jgroups.xml");
        canalRPC.connect(RPC_CLUSTER);

        // dispatcher ligado no canalRPC (server responde RPCs aqui)
        dispatcher = new RpcDispatcher(canalRPC, this);
        dispatcher.setReceiver(new Receiver() {
            @Override
            public void receive(Message msg) {
                // RPC não deveria nunca trazer MensagemCluster
            }
        });

        //dispatcher.setReceiver(this);

        log("SERVIDOR conectado ao cluster: " + canalCluster.getAddress());
        log("SERVIDOR RPC ativo em: " + canalRPC.getAddress());

//        // opcional: anunciar meu endereço RPC via canalCluster (ajuda gateways que só leem view)
//        MensagemCluster reg = MensagemCluster.registerRpc(canalRPC.getAddress().toString());
//        canalCluster.send(new ObjectMessage(null, reg));

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

    public String login(String username, String password) throws Exception {
        Usuario user = dados.buscarUsuarioPorUsername(username);

        if (user == null || !user.getPassword().equals(password))
            return null;

        // Gerar JWT
        return JwtUtil.gerarToken(username);
    }


    public List<String> listarArquivos() {
        log("LISTAR ARQUIVOS solicitado (RPC)");
        return dados.listarArquivos();
    }

    public boolean upload(String nome, byte[] conteudo) {
        try {
            log("UPLOAD solicitado (RPC): " + nome + " (" + conteudo.length + " bytes)");
            adquirirLock(nome);

            boolean ok = dados.salvarArquivo(nome, conteudo);
            if (!ok) {
                log("Falha ao gravar arquivo no disco: " + nome);
                return false;
            }

            metadata.put(nome, (long) conteudo.length);

            // replica para o cluster
            MensagemCluster msg = MensagemCluster.upload(nome, conteudo);
            canalCluster.send(new ObjectMessage(null, msg));

            log("UPLOAD concluído e replicado: " + nome);
            return true;

        } catch (Exception e) {
            log("ERRO NO UPLOAD: " + e.getMessage());
            e.printStackTrace();
            return false;
        } finally {
            liberarLock(nome);
        }
    }


    public byte[] download(String nome) {
        log("DOWNLOAD solicitado (RPC): " + nome);
        return dados.lerArquivo(nome);
    }

    public String gerarHashLocal() {
        log("🔐 HASH LOCAL solicitado (RPC)");
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");

            List<String> arquivos = dados.listarArquivos();
            Collections.sort(arquivos); // ✅ Ordenação garante hash consistente

            log("📊 Calculando hash de " + arquivos.size() + " arquivos...");

            for (String nomeArquivo : arquivos) {
                byte[] conteudo = dados.lerArquivo(nomeArquivo);

                if (conteudo != null) {
                    // Adiciona nome do arquivo ao hash (para detectar renomeações)
                    md.update(nomeArquivo.getBytes());
                    // Adiciona conteúdo ao hash
                    md.update(conteudo);
                }
            }

            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }

            String hash = sb.toString();
            log("✅ HASH LOCAL calculado: " + hash);
            return hash;

        } catch (Exception e) {
            log("❌ ERRO ao calcular hash: " + e.getMessage());
            return "ERRO-HASH";
        }
    }

    public synchronized boolean salvarUsuario(Usuario usuario) {
        try {
            boolean ok = dados.salvarUsuario(usuario);
            if (!ok) {
                log("ERRO ao salvar usuário localmente");
                return false;
            }
            MensagemCluster msg = MensagemCluster.salvarUsuario(usuario);
            canalCluster.send(new ObjectMessage(null, msg));
            log("Usuario salvo e replicado: " + usuario.getUsername());
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

        // 🔥 NOVO MEMBRO ENTRANDO → PUXA ESTADO DO CLUSTER
        try {
            if (!souLider()) {   // se não sou o líder, sou nó novo ou secundário
                log("Sou novo membro. Solicitando estado ao líder...");
                canalCluster.getState(null, 5000); // pega estado completo
                log("Estado recebido com sucesso!");
            }
        } catch (Exception e) {
            log("ERRO ao solicitar estado: " + e.getMessage());
        }
    }


    @Override
    public void getState(OutputStream out) throws Exception {
        log("═══════════════════════════════════════");
        log("📤 ENVIANDO ESTADO para novo membro...");

        try {
            EstadoCluster estado = new EstadoCluster();

            // 1️⃣ Metadata
            log("📊 Coletando metadata...");
            estado.metadata = new HashMap<>();
            synchronized (metadata) {
                estado.metadata.putAll(metadata);
            }
            log("   ✓ " + estado.metadata.size() + " metadados coletados");

            // 2️⃣ Arquivos
            log("📁 Coletando arquivos...");
            estado.arquivos = new HashMap<>();
            for (String nome : estado.metadata.keySet()) {
                try {
                    byte[] conteudo = dados.lerArquivo(nome);
                    if (conteudo != null) {
                        estado.arquivos.put(nome, conteudo);
                        log("   ✓ Arquivo: " + nome + " (" + conteudo.length + " bytes)");
                    } else {
                        log("   ⚠ Arquivo não encontrado: " + nome);
                    }
                } catch (Exception e) {
                    log("   ❌ Erro ao ler arquivo '" + nome + "': " + e.getMessage());
                }
            }
            log("   ✓ " + estado.arquivos.size() + " arquivos coletados");

            // 3️⃣ Usuários
            log("👥 Coletando usuários...");
            try {
                estado.usuarios = dados.listarUsuarios();
                if (estado.usuarios == null) {
                    estado.usuarios = Collections.emptyList();
                }
                log("   ✓ " + estado.usuarios.size() + " usuários coletados");

                for (Usuario u : estado.usuarios) {
                    log("   ✓ Usuário: " + u.getUsername());
                }

            } catch (Exception e) {
                log("   ⚠ Erro ao coletar usuários: " + e.getMessage());
                estado.usuarios = Collections.emptyList();
            }

            // 4️⃣ Serializar e enviar
            log("📦 Serializando estado...");
            DataOutputStream dos = new DataOutputStream(out);
            Util.objectToStream(estado, dos);
            dos.flush();

            log("✅ ESTADO ENVIADO COM SUCESSO!");
            log("   Resumo: " + estado.metadata.size() + " metadados, "
                    + estado.arquivos.size() + " arquivos, "
                    + estado.usuarios.size() + " usuários");
            log("═══════════════════════════════════════");

        } catch (Exception e) {
            log("❌ ERRO CRÍTICO ao enviar estado: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public void setState(InputStream in) throws Exception {
        log("Recebendo estado completo do cluster (aplicação incremental)...");
        EstadoCluster estado = (EstadoCluster) Util.objectFromStream(new DataInputStream(in));

        if (estado == null) {
            log("Estado recebido é nulo — nada a aplicar.");
            return;
        }

        // 1) Aplicar metadata (merge incremental)
        synchronized (metadata) {
            for (Map.Entry<String, Long> e : estado.metadata.entrySet()) {
                String nome = e.getKey();
                Long tamanhoRemoto = e.getValue();
                Long local = metadata.get(nome);

                // se não existir localmente ou tamanho diferente, marca para baixar (aplicado abaixo)
                if (local == null || !local.equals(tamanhoRemoto)) {
                    metadata.put(nome, tamanhoRemoto); // marcar novo/atualizado
                }
            }
        }

        // 2) Aplicar arquivos: só grava os que ainda não existem ou que diferem
        int arquivosAplicados = 0;
        for (Map.Entry<String, byte[]> e : estado.arquivos.entrySet()) {
            String nome = e.getKey();
            byte[] conteudo = e.getValue();

            try {
                // verifica se arquivo existe localmente e tem mesmo tamanho
                byte[] localConteudo = dados.lerArquivo(nome);
                boolean precisaSalvar = false;
                if (localConteudo == null) {
                    precisaSalvar = true;
                } else if (localConteudo.length != (conteudo == null ? 0 : conteudo.length)) {
                    precisaSalvar = true;
                }

                if (precisaSalvar) {
                    dados.salvarArquivo(nome, conteudo);
                    metadata.put(nome, (long) (conteudo == null ? 0 : conteudo.length));
                    arquivosAplicados++;
                }
            } catch (Exception ex) {
                log("Falha ao aplicar arquivo '" + nome + "': " + ex.getMessage());
            }
        }

        // 3) Aplicar usuários: adiciona somente usuários que ainda não existem localmente
        int usuariosAplicados = 0;
        try {
            for (Usuario u : estado.usuarios) {
                try {
                    // verificar existência por username
                    if (dados.buscarUsuarioPorUsername(u.getUsername()) == null) {
                        dados.salvarUsuario(u);
                        usuariosAplicados++;
                    }
                } catch (NoSuchMethodError | AbstractMethodError nsme) {
                    // Se DadosServer não expõe método de busca, tentamos salvar e ignorar falhas por duplicidade
                    try {
                        dados.salvarUsuario(u);
                        usuariosAplicados++;
                    } catch (Exception ignore) {}
                } catch (Exception ex) {
                    log("Erro ao aplicar usuário '" + u.getUsername() + "': " + ex.getMessage());
                }
            }
        } catch (NoSuchMethodError nm) {
            log("DadosServer não implementa listar/buscar usuários — pulei aplicação de usuários.");
        }

        log("Estado aplicado: " + arquivosAplicados + " arquivos novos/atualizados, "
                + usuariosAplicados + " usuários adicionados. Metadata total: " + metadata.size());
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
            log("📥 RECEBENDO replicação de USUÁRIO: " + m.usuario.getUsername());

            try {
                // Verifica se usuário já existe
                Usuario existente = dados.buscarUsuarioPorUsername(m.usuario.getUsername());
                if (existente != null) {
                    log("⚠️ Usuário JÁ EXISTE localmente, pulando: " + m.usuario.getUsername());
                    return;
                }

                // Salva o usuário replicado
                boolean ok = dados.replicarUsuario(m.usuario);

                if (ok) {
                    log("✅ REPLICAÇÃO DE USUÁRIO aplicada: " + m.usuario.getUsername());
                } else {
                    log("❌ FALHA ao aplicar replicação de usuário: " + m.usuario.getUsername());
                }
            } catch (Exception e) {
                log("❌ ERRO ao aplicar replicação de usuário: " + e.getMessage());
                e.printStackTrace();
            }
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
            metadata.put(f, Long.valueOf(content == null ? 0L : (long) content.length));
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
