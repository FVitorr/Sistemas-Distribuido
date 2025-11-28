package server;

import model.enums.Acao;

import java.io.Serializable;

public class MensagemCluster implements Serializable {
    public Acao acao;
    public String arquivo;
    public byte[] conteudo;

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


}
