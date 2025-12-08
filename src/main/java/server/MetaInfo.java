package server;

import java.io.Serializable;

public class MetaInfo implements Serializable {
    public String uuid;
    public long tamanho;

    public MetaInfo(String uuid, long tamanho) {
        this.uuid = uuid;
        this.tamanho = tamanho;
    }
}

