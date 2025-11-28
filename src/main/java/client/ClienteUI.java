package client;

import gateway.GatewayService;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Scanner;

public class ClienteUI {

    private GatewayService gateway;
    private Scanner scanner = new Scanner(System.in);

    public void start() {
        conectarComGateway();
        menuInicial();
        realizarLogin();
        menuPrincipal();
    }

    private void conectarComGateway() {
        try {
            Registry registry = LocateRegistry.getRegistry("localhost", 1099);
            gateway = (GatewayService) registry.lookup("Service");
            System.out.println("Cliente conectado ao Gateway!");
        } catch (Exception e) {
            throw new RuntimeException("Erro ao conectar com o gateway", e);
        }
    }

    // ★ NOVO MENU
    private void menuInicial() {
        while (true) {
            System.out.println("\n=== BEM-VINDO ===");
            System.out.println("1. Login");
            System.out.println("2. Criar conta");
            System.out.println("3. Sair");

            System.out.print("Escolha: ");
            String opc = scanner.nextLine();

            switch (opc) {
                case "1" -> { return; } // segue para login
                case "2" -> criarConta();
                case "3" -> System.exit(0);
                default -> System.out.println("Opção inválida.");
            }
        }
    }

    // ★ NOVA FUNÇÃO
    private void criarConta() {
        try {
            System.out.print("\nNovo usuário: ");
            String username = scanner.nextLine();

            System.out.print("Senha: ");
            String password = scanner.nextLine();

            boolean ok = gateway.criarConta(username, password);

            if (ok) {
                System.out.println("Conta criada com sucesso!");
            } else {
                System.out.println("Erro: usuário já existe.");
            }

        } catch (Exception e) {
            System.out.println("Erro ao criar conta: " + e.getMessage());
        }
    }

    private void realizarLogin() {
        boolean ok = false;

        while (!ok) {
            System.out.print("\nUsuário: ");
            String username = scanner.nextLine();

            System.out.print("Senha: ");
            String password = scanner.nextLine();

            try {
                ok = gateway.login(username, password);
                if (!ok) System.out.println("Credenciais inválidas. Tente novamente.");
            } catch (Exception e) {
                System.out.println("Erro ao tentar logar: " + e.getMessage());
            }
        }

        System.out.println("Login realizado com sucesso!");
    }

    private void menuPrincipal() {
        while (true) {
            System.out.println("\n=== MENU PRINCIPAL ===");
            System.out.println("1. Listar Arquivos");
            System.out.println("2. Upload");
            System.out.println("3. Download");
            System.out.println("4. Sair");

            System.out.print("Escolha: ");
            String opc = scanner.nextLine();

            switch (opc) {
                case "1" -> listarArquivos();
                case "2" -> upload();
                case "3" -> download();
                case "4" -> System.exit(0);
                default -> System.out.println("Opção inválida.");
            }

            mostrarHashRodape();
        }
    }

    private void listarArquivos() {
        try {
            List<String> arquivos = gateway.listarArquivos();
            System.out.println("\nArquivos disponíveis:");
            arquivos.forEach(System.out::println);
        } catch (Exception e) {
            System.out.println("Erro ao listar arquivos");
        }
    }

    private void upload() {
        try {
            System.out.print("Nome do arquivo: ");
            String nome = scanner.nextLine();

            System.out.print("Conteúdo: ");
            String conteudo = scanner.nextLine();

            boolean ok = gateway.upload(nome, conteudo.getBytes());

            System.out.println(ok ? "Upload feito!" : "Erro no upload.");

        } catch (Exception e) {
            System.out.println("Erro no upload");
        }
    }

    private void download() {
        try {
            System.out.print("Nome do arquivo: ");
            String nome = scanner.nextLine();

            byte[] conteudo = gateway.download(nome);

            if (conteudo == null) {
                System.out.println("Arquivo não encontrado.");
                return;
            }

            System.out.println("Conteúdo do arquivo:");
            System.out.println(new String(conteudo));

        } catch (Exception e) {
            System.out.println("Erro no download");
        }
    }

    private void mostrarHashRodape() {
        try {
            String hash = gateway.getSistemaHash();
            System.out.println("\n[HASH SISTEMA]: " + hash);
        } catch (Exception ignored) {}
    }
}
