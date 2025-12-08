package server;

import model.Usuario;
import org.jgroups.*;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Util;
import security.JwtUtil;

import java.io.*;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ControleServer implements Receiver, Closeable {

    private static final String CLUSTER = "FileServerCluster";
    private static final String RPC_CLUSTER = "FileServerRPC";
    private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

    protected JChannel canalCluster;
    protected JChannel canalRPC;
    protected RpcDispatcher dispatcher;
    protected DadosServer dados;

    private final Map<String, Long> metadata = new ConcurrentHashMap<>();
    private final Map<String, Boolean> arquivosBloqueados = new ConcurrentHashMap<>();
    private final Map<String, Queue<Address>> filasDeLock = new ConcurrentHashMap<>();

    // ✅ Controle de confirmações de upload (QUORUM)
    private final Map<String, Set<Address>> confirmacoesUpload = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> contagemConfirmacoes = new ConcurrentHashMap<>();


    // ✅ Controle de transações distribuídas
    private final Map<String, Set<Address>> transacoesAguardando = new ConcurrentHashMap<>();
    private final Map<String, Boolean> statusTransacao = new ConcurrentHashMap<>();

    protected Address lider;

    public static class EstadoCluster implements Serializable {
        private static final long serialVersionUID = 1L;
        public Map<String, Long> metadata;
        public Map<String, byte[]> arquivos;
        public List<Usuario> usuarios;
    }

    public ControleServer() throws Exception {
        this.dados = new DadosServer();

        canalCluster = new JChannel("jgroups.xml");
        canalCluster.setReceiver(this);
        canalCluster.connect(CLUSTER);

        canalRPC = new JChannel("jgroups.xml");
        canalRPC.connect(RPC_CLUSTER);

        dispatcher = new RpcDispatcher(canalRPC, this);
        dispatcher.setReceiver(new Receiver() {
            @Override
            public void receive(Message msg) {
                // RPC não processa MensagemCluster
            }
        });

        log("SERVIDOR conectado ao cluster: " + canalCluster.getAddress());
        log("SERVIDOR RPC ativo em: " + canalRPC.getAddress());

        atualizarMetadataLocal();
    }

    private void log(String msg) {
        System.out.println("[" + sdf.format(new Date()) + "] " + msg);
    }

    // =========================================================================
    //  RPC METHODS
    // =========================================================================

    public boolean isBackend() {
        return true;
    }

    public String login(String username, String password) throws Exception {
        log("LOGIN solicitado (RPC): " + username);
        Usuario user = dados.buscarUsuarioPorUsername(username);
        log("USER: " + user);
        if (user == null || !user.getPassword().equals(password))
            return null;
        return JwtUtil.gerarToken(username);
    }

    public List<String> listarArquivos() {
        log("LISTAR ARQUIVOS solicitado (RPC)");
        return dados.listarArquivos();
    }

    // =========================================================================
//  CORREÇÕES NO MÉTODO upload() DO ControleServer.java
// =========================================================================

    public boolean upload(String nome, byte[] conteudo) {
        String uploadId = UUID.randomUUID().toString();
        try {
            log("UPLOAD solicitado (RPC): " + nome + " (" + conteudo.length + " bytes) - uploadId=" + uploadId);
            adquirirLock(nome);

            // 1) Salva localmente
            boolean ok = dados.salvarArquivo(nome, conteudo);
            if (!ok) {
                log("Falha ao gravar arquivo no disco: " + nome);
                return false;
            }

            // 2) Atualiza metadata local
            metadata.put(nome, (long) conteudo.length);

            // 3) Inicializa estruturas de confirmação
            View view = canalCluster.getView();
            int totalServidores = view.size();
            int quorumNecessario = (totalServidores / 2) + 1;
            log("⭑ Total servidores: " + totalServidores + ", quorum necessário: " + quorumNecessario);

            confirmacoesUpload.put(uploadId, Collections.synchronizedSet(new HashSet<>()));
            contagemConfirmacoes.put(uploadId, new AtomicInteger(1)); // conta eu mesmo
            confirmacoesUpload.get(uploadId).add(canalCluster.getAddress());

            // 4) ✅ CORRIGIDO: Envia mensagem com endereço do coordenador
            MensagemCluster msg = MensagemCluster.upload(nome, conteudo, uploadId, canalCluster.getAddress());
            canalCluster.send(new ObjectMessage(null, msg));

            // 5) Aguarda confirmações até atingir quorum ou timeout
            long inicio = System.currentTimeMillis();
            long timeout = 15000;
            while (System.currentTimeMillis() - inicio < timeout) {
                int cont = contagemConfirmacoes.get(uploadId).get();
                if (cont >= quorumNecessario) {
                    log("✅ UPLOAD confirmado por quorum (" + cont + "/" + totalServidores + "): " + nome);
                    confirmacoesUpload.remove(uploadId);
                    contagemConfirmacoes.remove(uploadId);
                    return true;
                }
                Thread.sleep(100);
            }

            // 6) Timeout -> rollback
            log("❌ Timeout ao aguardar quorum para uploadId=" + uploadId + " (tentando rollback)");
            try {
                dados.deletarArquivo(nome);
            } catch (Exception ex) {
                log("⚠️ Erro ao deletar arquivo local no rollback: " + ex.getMessage());
            }
            metadata.remove(nome);

            MensagemCluster rollback = MensagemCluster.rollbackUpload(nome, uploadId);
            canalCluster.send(new ObjectMessage(null, rollback));

            confirmacoesUpload.remove(uploadId);
            contagemConfirmacoes.remove(uploadId);

            return false;

        } catch (Exception e) {
            log("ERRO NO UPLOAD: " + e.getMessage());
            e.printStackTrace();
            try { dados.deletarArquivo(nome); } catch (Exception ignored) {}
            metadata.remove(nome);
            return false;
        } finally {
            liberarLock(nome);
        }
    }

    private void aplicarUploadCluster(Message msg, MensagemCluster m) {
        synchronized (this) {
            log("📥 RECEBENDO replicação UPLOAD: " + m.arquivo);
            log("   Upload ID: " + m.uploadId);
            log("   Origem: " + m.serverOrigin);

            Address origin = encontrarMembroPorString(m.serverOrigin);


            try {
                // Salvar arquivo
                boolean ok = dados.salvarArquivo(m.arquivo, m.conteudo);

                if (ok) {
                    metadata.put(m.arquivo, (long) m.conteudo.length);
                    log("✅ Arquivo salvo: " + m.conteudo.length + " bytes");

                    enviarConfirmacaoUpload(m.uploadId, origin);
                } else {
                    log("❌ Falha ao salvar arquivo");
                    enviarConfirmacaoUploadNegativa(m.uploadId, origin);
                }

            } catch (Exception e) {
                log("❌ Erro ao aplicar upload: " + e.getMessage());
                enviarConfirmacaoUploadNegativa(m.uploadId, origin);
            }
        }
    }

    private Address encontrarMembroPorString(String addressStr) {
        if (addressStr == null) return null;

        View view = canalCluster.getView();
        for (Address addr : view.getMembers()) {
            if (addr.toString().equals(addressStr)) {
                return addr;
            }
        }
        return null;
    }

// =========================================================================
//  NOVO MÉTODO: Confirmação negativa
// =========================================================================

    private void enviarConfirmacaoUploadNegativa(String uploadId, Address serverOrigin) {
        try {
            MensagemCluster conf = MensagemCluster.confirmarUpload(uploadId,false);

            canalCluster.send(new ObjectMessage(serverOrigin, conf));
            log("📤 Confirmação NEGATIVA enviada para coordenador: " + serverOrigin);
        } catch (Exception e) {
            log("❌ Erro ao enviar confirmação negativa: " + e.getMessage());
        }
    }

// =========================================================================
//  CORREÇÃO NO MÉTODO receberConfirmacaoUpload()
// =========================================================================

    private void receberConfirmacaoUpload(Message msg, MensagemCluster m) {
        Set<Address> confirmados = confirmacoesUpload.get(m.uploadId);
        AtomicInteger contador = contagemConfirmacoes.get(m.uploadId);

        if (contador != null) {
            if (m.sucesso) {
                // ✅ Evita contar a mesma confirmação duas vezes
                if (confirmados != null && confirmados.add(msg.getSrc())) {
                    int novoValor = contador.incrementAndGet();
                    log("📊 Confirmação POSITIVA recebida de " + msg.getSrc() + " - Total: " + novoValor);
                }
            } else {
                log("⚠️ Confirmação NEGATIVA recebida de " + msg.getSrc() + " - Upload FALHOU");
                // ✅ Marca o upload como falho
                contador.set(-1000); // valor negativo indica falha
            }
        } else {
            log("⚠️ Confirmação recebida para uploadId desconhecido: " + m.uploadId);
        }
    }

    public boolean editaArquivo(String nome, byte[] conteudoNovo) {
        log("EDITAR ARQUIVO solicitado (RPC): " + nome + " (" + conteudoNovo.length + " bytes)");

        // Lê o conteúdo atual
        byte[] conteudoAtual = dados.lerArquivo(nome);

        if (conteudoAtual == null) {
            log("⚠️ Arquivo não existe, criando novo...");
            return upload(nome, conteudoNovo);
        }

        // Junta o conteúdo antigo + o novo
        byte[] combinado = new byte[conteudoAtual.length + conteudoNovo.length];
        System.arraycopy(conteudoAtual, 0, combinado, 0, conteudoAtual.length);
        System.arraycopy(conteudoNovo, 0, combinado, conteudoAtual.length, conteudoNovo.length);

        log("📌 Novo tamanho final: " + combinado.length + " bytes");

        // Reutiliza o mecanismo de upload (com lock + replicação)
        return upload(nome, combinado);
    }


    public byte[] download(String nome) {
        log("DOWNLOAD solicitado (RPC): " + nome);
        return dados.lerArquivo(nome);
    }

    public String gerarHashLocal() {
        log("🔐 HASH LOCAL solicitado (RPC)");
//        try {
//            MessageDigest md = MessageDigest.getInstance("SHA-256");
//
//            List<String> arquivos = dados.listarArquivos();
//
//            // ✅ Proteção contra NULL
//            if (arquivos == null) {
//                log("⚠️ Lista de arquivos retornou NULL");
//                arquivos = new ArrayList<>();
//            }
//
//            Collections.sort(arquivos);
//            log("📊 Calculando hash de " + arquivos.size() + " arquivos...");
//
//            // ✅ Hash vazio se não há arquivos
//            if (arquivos.isEmpty()) {
//                log("⚠️ Nenhum arquivo encontrado - retornando hash vazio");
//                byte[] digest = md.digest();
//                StringBuilder sb = new StringBuilder();
//                for (byte b : digest) {
//                    sb.append(String.format("%02x", b));
//                }
//                return sb.toString();
//            }
//
//            for (String nomeArquivo : arquivos) {
//                try {
//                    byte[] conteudo = dados.lerArquivo(nomeArquivo);
//
//                    if (conteudo != null) {
//                        md.update(nomeArquivo.getBytes());
//                        md.update(conteudo);
//                        log("   ✓ Hash de: " + nomeArquivo + " (" + conteudo.length + " bytes)");
//                    } else {
//                        log("   ⚠️ Arquivo NULL: " + nomeArquivo);
//                    }
//                } catch (Exception e) {
//                    log("   ❌ Erro ao ler arquivo '" + nomeArquivo + "': " + e.getMessage());
//                }
//            }
//
//            byte[] digest = md.digest();
//            StringBuilder sb = new StringBuilder();
//            for (byte b : digest) {
//                sb.append(String.format("%02x", b));
//            }
//
//            String hash = sb.toString();
//            log("✅ HASH LOCAL calculado: " + hash);
//            return hash;
//
//        } catch (Exception e) {
//            log("❌ ERRO CRÍTICO ao calcular hash: " + e.getMessage());
//            e.printStackTrace();
//            return "ERRO-HASH: " + e.getMessage();
//        }
        return "HASH-FUNCTION-DISABLED";
    }

    public boolean apagar(String nameFile){
        log("APAGAR ARQUIVO solicitado (RPC): " + nameFile);
        try {
            adquirirLock(nameFile);

            boolean ok = dados.deletarArquivo(nameFile);
            if (ok) {
                metadata.remove(nameFile);
                log("✅ Arquivo apagado: " + nameFile);

                try {
                    MensagemCluster msg = MensagemCluster.apagarArquivo(nameFile);
                    // Envia para todos (null) -> broadcast
                    canalCluster.send(new ObjectMessage(null, msg));
                    log("📤 Mensagem de APAGAR enviada ao cluster: " + nameFile);
                } catch (Exception e) {
                    log("❌ Erro ao enviar mensagem de APAGAR ao cluster: " + e.getMessage());
                    // Não falha a operação local só por causa da mensagem, mas registra
                }
            } else {
                log("⚠️ Falha ao apagar arquivo: " + nameFile);
            }
            return ok;
        } catch (Exception e) {
            log("❌ ERRO ao apagar arquivo: " + e.getMessage());
            return false;
        } finally {
            liberarLock(nameFile);
        }
    }

    public synchronized boolean salvarUsuario(Usuario usuario) {
        String transactionId = UUID.randomUUID().toString();

        try {
            log("════════════════════════════════════════");
            log("🔄 INICIANDO TRANSAÇÃO DISTRIBUÍDA");
            log("   Usuario: " + usuario.getUsername());
            log("   Transaction ID: " + transactionId);

            // Salvar localmente
            boolean ok = dados.salvarUsuario(usuario);
            if (!ok) {
                throw new RuntimeException("[LOCAL] Usuario invalido ou ja cadastrado");
            }
            log("✅ Salvo localmente");

            // Preparar replicação
            View view = canalCluster.getView();
            Set<Address> servidores = new HashSet<>();

            for (Address addr : view.getMembers()) {
                if (!addr.equals(canalCluster.getAddress())) {
                    servidores.add(addr);
                }
            }

            if (servidores.isEmpty()) {
                log("⚠️ Nenhum outro servidor - transação local apenas");
                log("════════════════════════════════════════");
                return true;
            }

            transacoesAguardando.put(transactionId, servidores);
            statusTransacao.put(transactionId, true);

            MensagemCluster msg = MensagemCluster.salvarUsuario(usuario, transactionId);
            canalCluster.send(new ObjectMessage(null, msg));

            // Aguardar confirmações
            long inicio = System.currentTimeMillis();
            while (System.currentTimeMillis() - inicio < 5000) {
                Set<Address> pendentes = transacoesAguardando.get(transactionId);

                if (pendentes == null || pendentes.isEmpty()) {
                    Boolean sucesso = statusTransacao.get(transactionId);

                    if (sucesso != null && sucesso) {
                        log("✅ TRANSAÇÃO CONCLUÍDA");
                        log("════════════════════════════════════════");
                        transacoesAguardando.remove(transactionId);
                        statusTransacao.remove(transactionId);
                        return true;
                    } else {
                        break;
                    }
                }
                Thread.sleep(100);
            }

            // Rollback
            log("🔙 Iniciando ROLLBACK...");
            dados.deletarUsuario(usuario.getUsername());

            MensagemCluster rollback = MensagemCluster.rollbackUsuario(
                    usuario.getUsername(), transactionId
            );
            canalCluster.send(new ObjectMessage(null, rollback));
            throw new RuntimeException("[Transação Abortada] Falha ao sincronizar usuário " + usuario.getUsername());
        } catch (Exception e) {
            log("❌ ERRO na transação: " + e.getMessage());
            try {
                dados.deletarUsuario(usuario.getUsername());
            } catch (Exception ignored) {}

            throw new RuntimeException("Falha ao criar usuário: " + e.getMessage(), e);
        }
    }

    private void aplicarRollbackUpload(MensagemCluster m) {
        synchronized (this) {
            log("🔙 RECEBENDO ROLLBACK de upload: " + m.arquivo);

            try {
                dados.deletarArquivo(m.arquivo);
                metadata.remove(m.arquivo);

                log("✅ Rollback de upload aplicado");
            } catch (Exception e) {
                log("❌ Erro no rollback: " + e.getMessage());
            }
        }
    }

    /**
     * ✅ Aplicar rollback de append
     */
    private void aplicarRollbackAppend(MensagemCluster m) {
        synchronized (this) {
            log("🔙 RECEBENDO ROLLBACK de append: " + m.arquivo);

            try {
                // Restaurar conteúdo anterior
                if (m.conteudo != null) {
                    dados.salvarArquivo(m.arquivo, m.conteudo);
                    metadata.put(m.arquivo, (long) m.conteudo.length);

                    log("✅ Rollback de append aplicado - Restaurado para " + m.conteudo.length + " bytes");
                } else {
                    log("⚠️ Conteúdo anterior NULL - não foi possível restaurar");
                }
            } catch (Exception e) {
                log("❌ Erro no rollback: " + e.getMessage());
            }
        }
    }

    // =========================================================================
    //  CALLBACKS JGROUPS
    // =========================================================================

    @Override
    public void receive(Message msg) {
        if (msg.getSrc() != null && msg.getSrc().equals(canalCluster.getAddress())) {
            return;
        }

        Object obj = msg.getObject();
        if (!(obj instanceof MensagemCluster)) return;
        MensagemCluster m = (MensagemCluster) obj;

        switch (m.acao) {
            case UPLOAD -> aplicarUploadCluster(msg, m);
            case LOCK_REQUEST -> processarPedidoDeLock(msg.getSrc(), m.arquivo);
            case LOCK_RELEASE -> processarLiberacaoDeLock(m.arquivo);
            case LOCK_CONCEDIDO -> receberLockConcedido(m.arquivo);
            case SALVAR_USUARIO -> aplicarSalvarUsuarioCluster(m);
            case ROLLBACK_USUARIO -> aplicarRollbackUsuario(m);
            case CONFIRMACAO_TRANSACAO -> receberConfirmacaoTransacao(msg, m);
            case ROLLBACK_UPLOAD -> aplicarRollbackUpload(m);
            case CONFIRMACAO_UPLOAD -> receberConfirmacaoUpload(msg, m);
            case APAGAR_ARQUIVO -> aplicarApagarCluster(m);
        }
    }

    @Override
    public void viewAccepted(View view) {
        log("═══════════════════════════════════════");
        log("🔄 NOVA VIEW DO CLUSTER");
        log("   Membros totais: " + view.size());

        Address novoLider = view.getCreator();
        boolean souNovoMembro = (lider == null);
        lider = novoLider;

        log("👑 LÍDER: " + lider);
        log("📋 MEMBROS:");
        for (int i = 0; i < view.getMembers().size(); i++) {
            Address addr = view.getMembers().get(i);
            String tipo = addr.equals(lider) ? " [LÍDER]" : "";
            String eu = addr.equals(canalCluster.getAddress()) ? " [EU]" : "";
            log("  [" + i + "] " + addr + tipo + eu);
        }
        log("═══════════════════════════════════════");

        if (souNovoMembro && !souLider()) {
            try {
                log("🆕 SOU NOVO MEMBRO - Solicitando estado...");
                Thread.sleep(500);
                canalCluster.getState(null, 10000);
                log("✅ Estado recebido!");
            } catch (Exception e) {
                log("❌ Erro ao solicitar estado: " + e.getMessage());
            }
        }
    }

    @Override
    public void getState(OutputStream out) throws Exception {
        log("📤 ENVIANDO ESTADO...");
        EstadoCluster estado = new EstadoCluster();

        estado.metadata = new HashMap<>();
        synchronized (metadata) {
            estado.metadata.putAll(metadata);
        }

        estado.arquivos = new HashMap<>();
        for (String nome : estado.metadata.keySet()) {
            byte[] conteudo = dados.lerArquivo(nome);
            if (conteudo != null) {
                estado.arquivos.put(nome, conteudo);
            }
        }

        estado.usuarios = dados.listarUsuarios();
        if (estado.usuarios == null) {
            estado.usuarios = Collections.emptyList();
        }

        Util.objectToStream(estado, new DataOutputStream(out));
        log("✅ Estado enviado: " + estado.arquivos.size() + " arquivos, " +
                estado.usuarios.size() + " usuários");
    }

    @Override
    public void setState(InputStream in) throws Exception {
        log("📥 RECEBENDO ESTADO...");
        EstadoCluster estado = (EstadoCluster) Util.objectFromStream(new DataInputStream(in));

        if (estado == null) return;

        synchronized (metadata) {
            metadata.clear();
            metadata.putAll(estado.metadata);
        }

        int arquivos = 0;
        for (Map.Entry<String, byte[]> e : estado.arquivos.entrySet()) {
            if (dados.salvarArquivo(e.getKey(), e.getValue())) {
                arquivos++;
            }
        }

        int usuarios = 0;
        for (Usuario u : estado.usuarios) {
            if (dados.buscarUsuarioPorUsername(u.getUsername()) == null) {
                if (dados.replicarUsuario(u)) {
                    usuarios++;
                }
            }
        }

        log("✅ Estado aplicado: " + arquivos + " arquivos, " + usuarios + " usuários");
    }

    // =========================================================================
    //  HANDLERS DE REPLICAÇÃO
    // =========================================================================


    /**
     * ✅ Enviar confirmação de upload para o coordenador
     */
    private void enviarConfirmacaoUpload(String uploadId, Address coordenador) {
        try {
            MensagemCluster conf = MensagemCluster.confirmarUpload(uploadId, true);
            canalCluster.send(new ObjectMessage(coordenador, conf));
            log("📤 Confirmação enviada para: " + coordenador);
        } catch (Exception e) {
            log("❌ Erro ao enviar confirmação: " + e.getMessage());
        }
    }

    private void aplicarApagarCluster(MensagemCluster m) {
        synchronized (this) {
            String nome = m.arquivo;
            log("📥 RECEBENDO APAGAR do cluster: " + nome + " (origem=" + m.serverOrigin + ")");
            try {
                boolean ok = dados.deletarArquivo(nome);
                metadata.remove(nome);
                if (ok) {
                    log("✅ Arquivo removido via cluster: " + nome);
                } else {
                    log("⚠️ Arquivo não encontrado/no deletado via cluster: " + nome);
                }
            } catch (Exception e) {
                log("❌ Erro ao aplicar APAGAR do cluster: " + e.getMessage());
            }
        }
    }

    private void aplicarSalvarUsuarioCluster(MensagemCluster m) {
        synchronized (this) {
            log("📥 RECEBENDO replicação USUÁRIO: " + m.usuario.getUsername());

            try {
                Usuario existente = dados.buscarUsuarioPorUsername(m.usuario.getUsername());
                if (existente != null) {
                    log("⚠️ Usuário já existe");
                    enviarConfirmacaoTransacao(m.transactionId, false);
                    return;
                }

                boolean ok = dados.replicarUsuario(m.usuario);
                enviarConfirmacaoTransacao(m.transactionId, ok);

                if (ok) {
                    log("✅ Replicação aplicada");
                } else {
                    log("❌ Falha na replicação");
                }
            } catch (Exception e) {
                log("❌ Erro: " + e.getMessage());
                enviarConfirmacaoTransacao(m.transactionId, false);
            }
        }
    }

    private void aplicarRollbackUsuario(MensagemCluster m) {
        log("🔙 RECEBENDO ROLLBACK: " + m.usuario.getUsername());
        try {
            dados.deletarUsuario(m.usuario.getUsername());
            log("✅ Rollback aplicado");
        } catch (Exception e) {
            log("❌ Erro no rollback: " + e.getMessage());
        }
    }

    private void enviarConfirmacaoTransacao(String txId, boolean sucesso) {
        try {
            MensagemCluster conf = MensagemCluster.confirmarTransacao(txId, sucesso);
            canalCluster.send(new ObjectMessage(lider, conf));
        } catch (Exception e) {
            log("❌ Erro ao enviar confirmação: " + e.getMessage());
        }
    }

    private void receberConfirmacaoTransacao(Message msg, MensagemCluster m) {
        Set<Address> pendentes = transacoesAguardando.get(m.transactionId);
        if (pendentes != null) {
            pendentes.remove(msg.getSrc());
            if (!m.sucesso) {
                statusTransacao.put(m.transactionId, false);
            }
        }
    }

    // =========================================================================
    //  LOCK DISTRIBUÍDO
    // =========================================================================

    private void adquirirLock(String arquivo) throws Exception {
        if (souLider()) {
            log("👑 Sou LÍDER. Bloqueando localmente: " + arquivo);
            bloquearOuEnfileirar(arquivo, canalCluster.getAddress());

            // ✅ LÍDER TAMBÉM PRECISA ESPERAR se o lock estiver ocupado
            long inicio = System.currentTimeMillis();
            while (!possoUsar(arquivo)) {
                if (System.currentTimeMillis() - inicio > 30000) {
                    throw new Exception("Timeout ao aguardar lock: " + arquivo);
                }
                Thread.sleep(100);
            }
            log("✅ LOCK CONCEDIDO (líder): " + arquivo);
            return;
        }

        log("Solicitando LOCK ao líder: " + arquivo);
        MensagemCluster req = MensagemCluster.solicitarLock(arquivo);
        canalCluster.send(new ObjectMessage(lider, req));

        long inicio = System.currentTimeMillis();
        while (!possoUsar(arquivo)) {
            if (System.currentTimeMillis() - inicio > 30000) {
                throw new Exception("Timeout ao aguardar lock: " + arquivo);
            }
            Thread.sleep(100);
        }
        log("✅ LOCK CONCEDIDO: " + arquivo);
    }

    private void liberarLock(String arquivo) {
        try {
            MensagemCluster m = MensagemCluster.liberarLock(arquivo);
            canalCluster.send(new ObjectMessage(null, m));
        } catch (Exception ignored) {}
    }

    private void bloquearOuEnfileirar(String arquivo, Address solicitante) {
        boolean bloqueado = arquivosBloqueados.getOrDefault(arquivo, false);
        Queue<Address> fila = filasDeLock.computeIfAbsent(arquivo, f -> new ArrayDeque<>());

        if (!bloqueado) {
            arquivosBloqueados.put(arquivo, true);
            fila.add(solicitante);
            log("✅ LOCK concedido imediatamente para " + solicitante);

            // Se não é para mim mesmo, notifica
            if (!solicitante.equals(canalCluster.getAddress())) {
                notificarLockConcedido(solicitante, arquivo);
            }
        } else {
            fila.add(solicitante);
            log("⏳ LOCK ocupado - enfileirando: " + solicitante + " (posição " + fila.size() + ")");
        }
    }

    private void processarPedidoDeLock(Address origem, String arquivo) {
        if (!souLider()) return;

        log("👑 LÍDER processando lock de " + origem + " para " + arquivo);
        bloquearOuEnfileirar(arquivo, origem);
    }

    private void processarLiberacaoDeLock(String arquivo) {
        if (!souLider()) return;

        Queue<Address> fila = filasDeLock.get(arquivo);
        if (fila == null || fila.isEmpty()) {
            log("⚠️ Tentativa de liberar lock sem fila: " + arquivo);
            return;
        }

        log("🔓 LÍDER liberando lock: " + arquivo);
        log("   Fila antes: " + fila.size() + " aguardando");

        // Remove quem estava usando
        Address atual = fila.poll();
        log("   Removido: " + atual);

        if (fila.isEmpty()) {
            // Ninguém mais aguardando - libera completamente
            arquivosBloqueados.put(arquivo, false);
            log("✅ Arquivo completamente liberado: " + arquivo);
        } else {
            // Próximo na fila recebe o lock
            Address proximo = fila.peek();
            log("➡️  Próximo na fila: " + proximo);

            // ✅ NOTIFICA o próximo (se não for o próprio líder)
            if (!proximo.equals(canalCluster.getAddress())) {
                notificarLockConcedido(proximo, arquivo);
            } else {
                log("   (próximo sou eu mesmo - não precisa notificar)");
            }
        }
    }

    private void notificarLockConcedido(Address destino, String arquivo) {
        try {
            MensagemCluster msg = MensagemCluster.lockConcedido(arquivo);
            canalCluster.send(new ObjectMessage(destino, msg));
        } catch (Exception e) {
            log("❌ Erro ao notificar lock: " + e.getMessage());
        }
    }

    private void receberLockConcedido(String arquivo) {
        log("📥 LOCK CONCEDIDO para " + arquivo);
        Queue<Address> fila = filasDeLock.computeIfAbsent(arquivo, f -> new ArrayDeque<>());
        if (fila.isEmpty()) {
            fila.add(canalCluster.getAddress());
        }
    }

    private boolean possoUsar(String arquivo) {
        Queue<Address> fila = filasDeLock.get(arquivo);
        return fila != null && !fila.isEmpty() && fila.peek().equals(canalCluster.getAddress());
    }

    private boolean souLider() {
        return lider != null && canalCluster.getAddress().equals(lider);
    }

    // =========================================================================
    //  UTILITÁRIOS
    // =========================================================================

    private void atualizarMetadataLocal() {
        for (String f : dados.listarArquivos()) {
            byte[] content = dados.lerArquivo(f);
            metadata.put(f, content == null ? 0L : (long) content.length);
        }
        log("Metadata carregada: " + metadata.size() + " arquivos");
    }

    @Override
    public void close() {
        if (dispatcher != null) dispatcher.stop();
        if (canalRPC != null) canalRPC.close();
        if (canalCluster != null) canalCluster.close();
    }

    public static void main(String[] args) {
        try {
            System.out.println("╔════════════════════════════════════════════╗");
            System.out.println("║     CONTROLE SERVER (JGroups Only)         ║");
            System.out.println("╚════════════════════════════════════════════╝");

            ControleServer server = new ControleServer();
            System.out.println("✓ Servidor conectado ao cluster e RPC");

            Thread.currentThread().join();

        } catch (Exception e) {
            System.err.println("❌ ERRO ao iniciar servidor:");
            e.printStackTrace();
        }
    }
}