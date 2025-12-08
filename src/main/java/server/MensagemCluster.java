package server;

import model.Usuario;
import org.jgroups.Address;

import java.io.Serializable;
import java.util.UUID;

public class MensagemCluster implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum Acao {
        UPLOAD,
        CONFIRMACAO_UPLOAD,      // ✅ Confirmação de upload recebido
        ROLLBACK_UPLOAD,         // ✅ Desfazer upload
        LOCK_REQUEST,
        LOCK_RELEASE,
        LOCK_CONCEDIDO,
        SALVAR_USUARIO,
        ROLLBACK_USUARIO,
        CONFIRMACAO_TRANSACAO,
        REGISTER_RPC_ADDRESS,
        APAGAR_ARQUIVO,
    }

    public Acao acao;
    public String uuid;              // ✅ ID do arquivo (se necessário)
    public String uploadId;          // ✅ ID para rastreamento de upload
    public String arquivo;
    public byte[] conteudo;
    public Usuario usuario;
    public String rpcAddress;
    public boolean replicado = false;
    public String serverOrigin = null;

    // Campos para controle de transações
    public String transactionId;
    public boolean sucesso;

    private MensagemCluster() {}

    // ================== MÉTODOS DE ARQUIVO ==================

    /**
     * Upload de arquivo com ID para rastreamento
     */
    public static MensagemCluster upload(String arquivo, byte[] conteudo, String uploadId, Address serverOrigin) {
        MensagemCluster m = new MensagemCluster();
        m.acao = Acao.UPLOAD;
        m.arquivo = arquivo;
        m.conteudo = conteudo;
        m.uploadId = uploadId;  // ✅ Usa uploadId para rastrear confirmações
        m.serverOrigin = serverOrigin.toString();
        return m;
    }

    /**
     * Confirmação de que o upload foi recebido e salvo
     */
    public static MensagemCluster confirmarUpload(String uploadId, boolean sucesso) {
        MensagemCluster m = new MensagemCluster();
        m.acao = Acao.CONFIRMACAO_UPLOAD;
        m.uploadId = uploadId;
        m.sucesso = sucesso;
        return m;
    }

    /**
     * Rollback de upload (desfazer em todas as réplicas)
     */
    public static MensagemCluster rollbackUpload(String arquivo, String uploadId) {
        MensagemCluster m = new MensagemCluster();
        m.acao = Acao.ROLLBACK_UPLOAD;
        m.arquivo = arquivo;
        m.uploadId = uploadId;
        return m;
    }

    public static MensagemCluster apagarArquivo(String nameFile) {
        MensagemCluster m = new MensagemCluster();
        m.acao = Acao.APAGAR_ARQUIVO;
        m.arquivo = nameFile;
        return m;
    }

    // ================== MÉTODOS DE LOCK ==================

    public static MensagemCluster solicitarLock(String arquivo) {
        MensagemCluster m = new MensagemCluster();
        m.acao = Acao.LOCK_REQUEST;
        m.arquivo = arquivo;
        return m;
    }

    public static MensagemCluster liberarLock(String arquivo) {
        MensagemCluster m = new MensagemCluster();
        m.acao = Acao.LOCK_RELEASE;
        m.arquivo = arquivo;
        return m;
    }

    /**
     * Notificação de que lock foi concedido
     */
    public static MensagemCluster lockConcedido(String arquivo) {
        MensagemCluster m = new MensagemCluster();
        m.acao = Acao.LOCK_CONCEDIDO;
        m.arquivo = arquivo;
        return m;
    }

    // ================== MÉTODOS DE USUÁRIO ==================

    public static MensagemCluster salvarUsuario(Usuario u, String transactionId) {
        MensagemCluster m = new MensagemCluster();
        m.acao = Acao.SALVAR_USUARIO;
        m.usuario = u;
        m.transactionId = transactionId;
        return m;
    }

    public static MensagemCluster rollbackUsuario(String username, String transactionId) {
        MensagemCluster m = new MensagemCluster();
        m.acao = Acao.ROLLBACK_USUARIO;
        m.usuario = new Usuario();
        m.usuario.setUsername(username);
        m.transactionId = transactionId;
        return m;
    }

    public static MensagemCluster confirmarTransacao(String transactionId, boolean sucesso) {
        MensagemCluster m = new MensagemCluster();
        m.acao = Acao.CONFIRMACAO_TRANSACAO;
        m.transactionId = transactionId;
        m.sucesso = sucesso;
        return m;
    }

    // ================== OUTROS ==================

    public static MensagemCluster registerRpc(String rpcAddr) {
        MensagemCluster m = new MensagemCluster();
        m.acao = Acao.REGISTER_RPC_ADDRESS;
        m.rpcAddress = rpcAddr;
        return m;
    }

    @Override
    public String toString() {
        return "MensagemCluster{" +
                "acao=" + acao +
                (arquivo != null ? ", arquivo=" + arquivo : "") +
                (uploadId != null ? ", uploadId=" + uploadId : "") +
                (usuario != null ? ", usuario=" + usuario.getUsername() : "") +
                (rpcAddress != null ? ", rpcAddr=" + rpcAddress : "") +
                (transactionId != null ? ", txId=" + transactionId : "") +
                ", replicado=" + replicado +
                ", sucesso=" + sucesso +
                '}';
    }
}