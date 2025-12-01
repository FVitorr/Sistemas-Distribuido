package server;

import model.Usuario;
import java.io.Serializable;

public class MensagemCluster implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum Acao {
        UPLOAD,
        LOCK_REQUEST,
        LOCK_RELEASE,
        SALVAR_USUARIO,
        REGISTER_RPC_ADDRESS // RPC address como string
    }

    public Acao acao;
    public String arquivo;
    public byte[] conteudo;
    public Usuario usuario;
    public String rpcAddress; // serializável agora
    public boolean replicado = false;

    private MensagemCluster() {}

    public static MensagemCluster upload(String arquivo, byte[] conteudo) {
        MensagemCluster m = new MensagemCluster();
        m.acao = Acao.UPLOAD;
        m.arquivo = arquivo;
        m.conteudo = conteudo;
        return m;
    }

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

    public static MensagemCluster salvarUsuario(Usuario u) {
        MensagemCluster m = new MensagemCluster();
        m.acao = Acao.SALVAR_USUARIO;
        m.usuario = u;
        return m;
    }

    public static MensagemCluster registerRpc(String rpcAddr) {
        MensagemCluster m = new MensagemCluster();
        m.acao = Acao.REGISTER_RPC_ADDRESS;
        m.rpcAddress = rpcAddr; // agora é String
        return m;
    }

    @Override
    public String toString() {
        return "MensagemCluster{" +
                "acao=" + acao +
                (arquivo != null ? ", arquivo=" + arquivo : "") +
                (usuario != null ? ", usuario=" + usuario.getUsername() : "") +
                (rpcAddress != null ? ", rpcAddr=" + rpcAddress : "") +
                ", replicado=" + replicado +
                '}';
    }
}
