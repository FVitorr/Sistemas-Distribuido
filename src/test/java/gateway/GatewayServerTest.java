package gateway;

import database.UsuarioDAO;
import model.Usuario;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import server.ControleService;

import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class GatewayServerTest {

    private UsuarioDAO usuarioDAO;
    private ControleService controleService;
    private GatewayServer gateway;

    @BeforeEach
    void setup() throws Exception {
        // Mock do DAO e do ControleService
        usuarioDAO = mock(UsuarioDAO.class);
        controleService = mock(ControleService.class);

        // Criar uma classe anônima de GatewayServer que usa os mocks
        gateway = new GatewayServer(usuarioDAO, controleService);
    }

    @Test
    void testLoginSucesso() throws RemoteException {
        Usuario user = new Usuario("joao", "1234");
        when(usuarioDAO.buscarPorUsername("joao")).thenReturn(user);

        assertTrue(gateway.login("joao", "1234"));
    }

    @Test
    void testLoginFalhaSenha() throws RemoteException {
        Usuario user = new Usuario("joao", "1234");
        when(usuarioDAO.buscarPorUsername("joao")).thenReturn(user);

        assertFalse(gateway.login("joao", "senhaerrada"));
    }

    @Test
    void testLoginUsuarioNaoExiste() throws RemoteException {
        when(usuarioDAO.buscarPorUsername("maria")).thenReturn(null);

        assertFalse(gateway.login("maria", "1234"));
    }

    @Test
    void testListarArquivos() throws RemoteException {
        List<String> arquivos = Arrays.asList("teste.txt", "foto.png");
        when(controleService.listarArquivos()).thenReturn(arquivos);

        List<String> resultado = gateway.listarArquivos();
        assertEquals(arquivos, resultado);
    }

    @Test
    void testUploadEDownload() throws RemoteException {
        byte[] conteudo = "conteudo do arquivo".getBytes();

        when(controleService.upload("arquivo.txt", conteudo)).thenReturn(true);
        when(controleService.download("arquivo.txt")).thenReturn(conteudo);

        assertTrue(gateway.upload("arquivo.txt", conteudo));
        assertArrayEquals(conteudo, gateway.download("arquivo.txt"));
    }

    @Test
    void testGetSistemaHash() throws RemoteException {
        when(controleService.gerarHashGlobal()).thenReturn("HASH123");

        assertEquals("HASH123", gateway.getSistemaHash());
    }

    @Test
    void testCriarConta() {
        Usuario user = new Usuario("ana", "pass");
        when(usuarioDAO.salvar(any(Usuario.class))).thenReturn(true);

        assertTrue(gateway.criarConta("ana", "pass"));
        verify(usuarioDAO, times(1)).salvar(any(Usuario.class));
    }
}
