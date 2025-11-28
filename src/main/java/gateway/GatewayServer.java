package gateway;

import database.UsuarioDAO;
import lombok.AllArgsConstructor;
import lombok.Getter;
import model.Usuario;
import server.ControleService; // interface RMI do ControleServer

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;

@Getter
@AllArgsConstructor
public class GatewayServer implements GatewayService {

    private final UsuarioDAO usuarioDAO;
    private final ControleService controleService; // cliente RMI do ControleServer

    public GatewayServer() throws Exception {
        // Conecta ao ControleServer via RMI
        this.usuarioDAO = new UsuarioDAO();
        Registry registry = LocateRegistry.getRegistry("localhost", 1098); // IP/porta do ControleServer
        controleService = (ControleService) registry.lookup("ControleService");
    }

    @Override
    public boolean login(String username, String password) throws RemoteException {
        Usuario u = usuarioDAO.buscarPorUsername(username);
        return u != null && u.getPassword().equals(password);
    }

    @Override
    public List<String> listarArquivos() throws RemoteException {
        return controleService.listarArquivos(); // chama RMI do ControleServer
    }

    @Override
    public boolean upload(String nomeArquivo, byte[] conteudo) throws RemoteException {
        System.out.println("UPLOAD recebido no Gateway: " + nomeArquivo);
        return controleService.upload(nomeArquivo, conteudo); // RMI
    }

    @Override
    public byte[] download(String nomeArquivo) throws RemoteException {
        System.out.println("DOWNLOAD solicitado no Gateway: " + nomeArquivo);
        return controleService.download(nomeArquivo); // RMI
    }

    @Override
    public String getSistemaHash() throws RemoteException {
        return controleService.gerarHashGlobal(); // RMI
    }

    @Override
    public boolean criarConta(String user, String pass) {
        return usuarioDAO.salvar(new Usuario(user, pass));
    }

    public static void main(String[] args) {
        try {
            GatewayServer gateway = new GatewayServer();

            GatewayService stub =
                    (GatewayService) UnicastRemoteObject.exportObject(gateway, 0);

            Registry registry = LocateRegistry.createRegistry(1099);
            registry.rebind("Service", stub);

            System.out.println("Gateway iniciado na porta 1099!");
            Thread.currentThread().join(); // mantém o gateway vivo
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
