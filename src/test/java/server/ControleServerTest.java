package server;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ControleServerTest {

    private ControleServer controle;
    private DadosServer dadosMock;
    private JChannel canalClusterMock;
    private JChannel canalRPCMock;
    private Address liderMock;

    @BeforeEach
    void setup() throws Exception {
        dadosMock = mock(DadosServer.class);
        canalClusterMock = mock(JChannel.class);
        canalRPCMock = mock(JChannel.class);
        liderMock = mock(Address.class);

        when(canalClusterMock.getAddress()).thenReturn(liderMock);

        controle = new ControleServer(dadosMock) {
            {
                this.canalCluster = canalClusterMock;
                this.canalRPC = canalRPCMock;
                this.lider = liderMock;
            }
        };
    }

    @Test
    void testListarArquivos() {
        List<String> arquivos = Arrays.asList("arquivo1.txt", "arquivo2.txt");
        when(dadosMock.listarArquivos()).thenReturn(arquivos);

        List<String> resultado = controle.listarArquivos();

        assertEquals(arquivos, resultado);
    }

    @Test
    void testUploadSucesso() throws Exception {
        byte[] conteudo = "conteudo".getBytes();
        when(dadosMock.salvarArquivo("arquivo.txt", conteudo)).thenReturn(true);

        boolean ok = controle.upload("arquivo.txt", conteudo);

        assertTrue(ok);
        verify(dadosMock, times(1)).salvarArquivo("arquivo.txt", conteudo);
        verify(canalClusterMock, atLeastOnce()).send(any(Message.class));
    }

    @Test
    void testUploadFalha() throws Exception {
        byte[] conteudo = "conteudo".getBytes();
        when(dadosMock.salvarArquivo("arquivo.txt", conteudo)).thenReturn(false);

        boolean ok = controle.upload("arquivo.txt", conteudo);

        assertFalse(ok);
        verify(dadosMock, times(1)).salvarArquivo("arquivo.txt", conteudo);
        verify(canalClusterMock, atLeastOnce()).send(any(Message.class)); // liberarLock envia sempre
    }

    @Test
    void testDownload() {
        byte[] conteudo = "dados".getBytes();
        when(dadosMock.lerArquivo("arquivo.txt")).thenReturn(conteudo);

        byte[] resultado = controle.download("arquivo.txt");

        assertArrayEquals(conteudo, resultado);
    }

    @Test
    void testGerarHashGlobal() throws Exception {
        byte[] file1 = "aaa".getBytes();
        byte[] file2 = "bbb".getBytes();
        when(dadosMock.listarArquivos()).thenReturn(Arrays.asList("f1.txt", "f2.txt"));
        when(dadosMock.lerArquivo("f1.txt")).thenReturn(file1);
        when(dadosMock.lerArquivo("f2.txt")).thenReturn(file2);

        String hash = controle.gerarHashGlobal();

        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(file1);
        md.update(file2);
        byte[] expected = md.digest();

        StringBuilder sb = new StringBuilder();
        for (byte b : expected) sb.append(String.format("%02x", b));
        assertEquals(sb.toString(), hash);
    }
}
