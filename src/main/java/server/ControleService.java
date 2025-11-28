package server;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface ControleService extends Remote {
    List<String> listarArquivos() throws RemoteException;
    boolean upload(String nome, byte[] conteudo) throws RemoteException;
    byte[] download(String nome) throws RemoteException;
    String gerarHashGlobal() throws RemoteException;
}

