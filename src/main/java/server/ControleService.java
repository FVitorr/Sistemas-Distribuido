package server;

import model.Usuario;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface ControleService extends Remote {
    boolean login(String username, String password) throws RemoteException;

    List<String> listarArquivos() throws RemoteException;
    boolean upload(String nome, byte[] conteudo) throws RemoteException;
    byte[] download(String nome) throws RemoteException;
    String gerarHashLocal() throws RemoteException;

    boolean salvarUsuario(Usuario usuario) throws RemoteException; // ✅ ADICIONE AQUI
    boolean isBackend();
}