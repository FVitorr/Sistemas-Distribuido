package server;

import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

/**
 * DadosServer
 * Responsável apenas por:
 *  - Salvar arquivos no disco
 *  - Ler arquivos
 *  - Listar arquivos
 *
 * Essa classe não envolve comunicação distribuída.
 * Toda a lógica de cluster fica no ControleServer.
 */
public class DadosServer {

    private final Path pastaArquivos;

    public DadosServer() throws IOException {
        this.pastaArquivos = Paths.get("arquivos");

        // Cria pasta se não existir
        if (!Files.exists(pastaArquivos)) {
            Files.createDirectories(pastaArquivos);
        }
    }

    /**
     * Salva um arquivo no diretório local do servidor.
     */
    public boolean salvarArquivo(String nome, byte[] conteudo) {
        try {
            Path destino = pastaArquivos.resolve(nome);
            Files.write(destino, conteudo, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            return true;
        } catch (IOException e) {
            System.err.println("Erro ao salvar arquivo: " + nome);
            return false;
        }
    }

    /**
     * Lê um arquivo.
     */
    public byte[] lerArquivo(String nome) {
        try {
            Path arquivo = pastaArquivos.resolve(nome);

            if (!Files.exists(arquivo)) {
                return null;
            }

            return Files.readAllBytes(arquivo);

        } catch (IOException e) {
            System.err.println("Erro ao ler arquivo: " + nome);
            return null;
        }
    }

    /**
     * Lista todos os arquivos armazenados no servidor.
     */
    public List<String> listarArquivos() {
        List<String> lista = new ArrayList<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(pastaArquivos)) {
            for (Path p : stream) {
                if (Files.isRegularFile(p)) {
                    lista.add(p.getFileName().toString());
                }
            }
        } catch (IOException e) {
            System.err.println("Erro ao listar arquivos.");
        }

        return lista;
    }
}
