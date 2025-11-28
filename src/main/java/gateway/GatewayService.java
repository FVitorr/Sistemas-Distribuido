package gateway;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface GatewayService extends Remote {

    boolean login(String username, String password) throws RemoteException;

    List<String> listarArquivos() throws RemoteException;

    boolean upload(String nomeArquivo, byte[] conteudo) throws RemoteException;

    byte[] download(String nomeArquivo) throws RemoteException;

    String getSistemaHash() throws RemoteException;

    boolean criarConta(String username, String password) throws RemoteException;
}
