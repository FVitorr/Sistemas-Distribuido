package gateway;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface GatewayService extends Remote {

    String login(String username, String password) throws RemoteException;

    List<String> listarArquivos(String token) throws RemoteException;

    boolean upload(String nomeArquivo, byte[] conteudo) throws RemoteException;

    byte[] download(String nomeArquivo) throws RemoteException;

    String getSistemaHash() throws RemoteException;

    boolean criarConta(String username, String password) throws RemoteException;

    boolean editaArquivo(String nomeArquivo, byte[] conteudo) throws RemoteException;

    boolean apagar(String nome)  throws RemoteException;
}
