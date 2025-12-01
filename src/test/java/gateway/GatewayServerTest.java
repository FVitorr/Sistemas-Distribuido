package gateway;

import model.Usuario;
import org.jgroups.Address;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.RspList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class GatewayServerTest {

    private GatewayServer gateway;
    private RpcDispatcher dispatcherMock;
    private Address servidorMock;

    @BeforeEach
    void setup() throws Exception {
        // Mock do dispatcher e servidor
        dispatcherMock = mock(RpcDispatcher.class);
        servidorMock = mock(Address.class);

        // Instancia GatewayServer com dispatcher mockado
        gateway = new GatewayServer() {
            {
                this.dispatcher = dispatcherMock;
                this.servidoresAtivos.clear();
                this.servidoresAtivos.add(servidorMock);
            }

            @Override
            protected void log(String msg) {
                // ignora logs durante teste
            }
        };
    }

    // Helper para mockar RspList
    private RspList<Object> criarRspList(Object retorno) {
        RspList<Object> rsp = mock(RspList.class);
        when(rsp.isEmpty()).thenReturn(false);
        when(rsp.isReceived(servidorMock)).thenReturn(true);
        when(rsp.getValue(servidorMock)).thenReturn(retorno);
        return rsp;
    }

    @Test
    void testLoginSucesso() throws Exception {
        when(dispatcherMock.callRemoteMethods(
                eq(List.of(servidorMock)),
                any(MethodCall.class),
                any(RequestOptions.class)
        )).thenReturn(criarRspList(true));

        assertTrue(gateway.login("joao", "1234"));
    }

    @Test
    void testLoginFalha() throws Exception {
        when(dispatcherMock.callRemoteMethods(
                eq(List.of(servidorMock)),
                any(MethodCall.class),
                any(RequestOptions.class)
        )).thenReturn(criarRspList(false));

        assertFalse(gateway.login("joao", "senhaerrada"));
    }

    @Test
    void testListarArquivos() throws Exception {
        List<String> arquivos = Arrays.asList("a.txt", "b.txt");
        when(dispatcherMock.callRemoteMethods(
                eq(List.of(servidorMock)),
                argThat(call -> call.getName().equals("listarArquivos")),
                any(RequestOptions.class)
        )).thenReturn(criarRspList(arquivos));

        List<String> resultado = gateway.listarArquivos();
        assertEquals(arquivos, resultado);
    }

    @Test
    void testUploadEDownload() throws Exception {
        byte[] conteudo = "teste".getBytes();

        // Mock upload
        when(dispatcherMock.callRemoteMethods(
                eq(List.of(servidorMock)),
                argThat(call -> call.getName().equals("upload")),
                any(RequestOptions.class)
        )).thenReturn(criarRspList(true));

        // Mock download
        when(dispatcherMock.callRemoteMethods(
                eq(List.of(servidorMock)),
                argThat(call -> call.getName().equals("download")),
                any(RequestOptions.class)
        )).thenReturn(criarRspList(conteudo));

        assertTrue(gateway.upload("arquivo.txt", conteudo));
        assertArrayEquals(conteudo, gateway.download("arquivo.txt"));
    }

    @Test
    void testGetSistemaHash() throws Exception {
        when(dispatcherMock.callRemoteMethods(
                eq(List.of(servidorMock)),
                argThat(call -> call.getName().equals("gerarHashGlobal")),
                any(RequestOptions.class)
        )).thenReturn(criarRspList("HASH123"));

        assertEquals("HASH123", gateway.getSistemaHash());
    }

    @Test
    void testCriarConta() throws Exception {
        when(dispatcherMock.callRemoteMethods(
                eq(List.of(servidorMock)),
                argThat(call -> call.getName().equals("salvarUsuario")),
                any(RequestOptions.class)
        )).thenReturn(criarRspList(true));

        assertTrue(gateway.criarConta("ana", "pass"));
    }
}
