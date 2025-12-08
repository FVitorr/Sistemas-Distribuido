package server;

import database.JPAUtil;
import database.UsuarioDAO;
import model.Usuario;
import java.io.*;
import java.nio.file.*;
import java.util.*;

public class DadosServer {

    private final String diretorio;
    private final UsuarioDAO usuarioDAO;

    public DadosServer() {
        this(
                System.getProperty("app.storage", "./storage-" + UUID.randomUUID()),
                System.getProperty("app.db", "usuarios-" + UUID.randomUUID() + ".db")
        );
    }

    public DadosServer(String diretorio, String nomeBanco) {
        this.diretorio = diretorio;
        JPAUtil.init(nomeBanco);
        this.usuarioDAO = new UsuarioDAO();

        try {
            Files.createDirectories(Paths.get(diretorio));
        } catch (IOException e) {
            System.err.println("Erro ao criar diretório: " + e.getMessage());
        }
    }

    public DadosServer(String diretorio) {
        this(diretorio, "usuarios-" + UUID.randomUUID() + ".db");
    }

    // =========================================================================
    //  MÉTODOS DE ARQUIVOS
    // =========================================================================

    public boolean salvarArquivo(String nome, byte[] conteudo) {
        try {
            Path caminho = Paths.get(diretorio, nome);
            Files.write(caminho, conteudo);
            return true;
        } catch (IOException e) {
            System.err.println("Erro ao salvar arquivo: " + e.getMessage());
            return false;
        }
    }

    public byte[] lerArquivo(String nome) {
        try {
            Path caminho = Paths.get(diretorio, nome);
            return Files.readAllBytes(caminho);
        } catch (IOException e) {
            System.err.println("Erro ao ler arquivo: " + e.getMessage());
            return null;
        }
    }

    public List<String> listarArquivos() {
        try {
            return Files.list(Paths.get(diretorio))
                    .filter(Files::isRegularFile)
                    .map(p -> p.getFileName().toString())
                    .toList();
        } catch (IOException e) {
            return Collections.emptyList();
        }
    }

    // =========================================================================
    //  MÉTODOS DE USUÁRIOS
    // =========================================================================

    /**
     * Salva novo usuário (usa persist)
     */
    public boolean salvarUsuario(Usuario usuario) {
        try{
            return usuarioDAO.salvar(usuario);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Replica usuário de outro servidor (usa merge)
     */
    public boolean replicarUsuario(Usuario usuario) {
        return usuarioDAO.replicarUsuario(usuario);
    }

    /**
     * Deleta usuário (usado para rollback de transações)
     */
    public boolean deletarUsuario(String username) {
        return usuarioDAO.deletar(username);
    }

    /**
     * Valida credenciais de login
     */
    public boolean validarUsuario(String username, String password) {
        Usuario usuario = usuarioDAO.buscarPorUsername(username);
        return usuario != null && usuario.getPassword().equals(password);
    }

    /**
     * Lista todos os usuários
     */
    public List<Usuario> listarUsuarios() {
        return usuarioDAO.listarTodos();
    }

    /**
     * Busca usuário por username
     */
    public Usuario buscarUsuarioPorUsername(String username) {
        return usuarioDAO.buscarPorUsername(username);
    }

    public boolean deletarArquivo(String nome) {
        try {
            Path caminho = Paths.get(diretorio, nome);
            return Files.deleteIfExists(caminho);
        } catch (IOException e) {
            System.err.println("Erro ao deletar arquivo: " + e.getMessage());
            return false;
        }
    }
}