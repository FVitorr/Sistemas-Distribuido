package gateway;

import model.Usuario;
import org.jgroups.*;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.RspList;

import java.io.Closeable;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Gateway com Load Balancer Round-Robin
 * Usa RpcDispatcher para chamar métodos nos servidores
 */
public class GatewayServer implements GatewayService, Receiver, Closeable {

    private static final String CLUSTER = "FileServerRPC";

    private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

    private JChannel canal;
    private RpcDispatcher dispatcher; // ✅ Para chamar métodos nos servidores
    private List<Address> servidoresAtivos;
    private AtomicInteger roundRobinIndex;

    public GatewayServer() throws Exception {
        servidoresAtivos = new ArrayList<>();
        roundRobinIndex = new AtomicInteger(0);

        canal = new JChannel("jgroups.xml");

        // ✅ IMPORTANTE: Registrar ANTES de conectar
        //canal.setReceiver(this);

        canal.connect(CLUSTER);

        log("GATEWAY conectado ao cluster: " + canal.getAddress());
        log("🔍 View inicial: " + canal.getView());

        // Criar dispatcher DEPOIS
        dispatcher = new RpcDispatcher(canal, null);
        dispatcher.setReceiver(this);

        log("✅ Receiver registrado");

        atualizarListaServidores();
    }



    private void log(String msg) {
        System.out.println("[GATEWAY " + sdf.format(new Date()) + "] " + msg);
    }

    // =========================================================================
    //  LOAD BALANCER - Round Robin
    // =========================================================================

    private Address selecionarServidor() {
        if (servidoresAtivos.isEmpty()) {
            log("❌ ERRO: Nenhum servidor disponível!");
            return null;
        }
        int index = roundRobinIndex.getAndIncrement() % servidoresAtivos.size();
        Address servidor = servidoresAtivos.get(index);
        log("⚖️ Load Balancer → " + servidor);
        return servidor;
    }

    /**
     * Chama método remoto no servidor selecionado
     */
    private Object chamarMetodoRemoto(String nomeMetodo, Object[] args, Class[] tipos)
            throws Exception {

        Address servidor = selecionarServidor();
        if (servidor == null) {
            throw new RemoteException("Nenhum servidor disponível");
        }

        // GET_FIRST retorna apenas a primeira resposta recebida
        RequestOptions opts = new RequestOptions(ResponseMode.GET_FIRST, 5000);

        MethodCall call = new MethodCall(nomeMetodo, args, tipos);

        // Passa apenas o endereço do servidor escolhido
        RspList<Object> respostas = dispatcher.callRemoteMethods(
                Collections.singletonList(servidor),
                call,
                opts
        );

        if (!respostas.isReceived(servidor)) {
            throw new Exception("Servidor não respondeu: " + servidor);
        }

        return respostas.getValue(servidor);
    }


    /**
     * Retry automático em caso de falha
     */
    private Object chamarComRetry(String metodo, Object[] args, Class[] tipos, int maxTentativas)
            throws RemoteException {

        Exception ultimoErro = null;

        for (int i = 0; i < maxTentativas; i++) {
            try {
                return chamarMetodoRemoto(metodo, args, tipos);
            } catch (Exception e) {
                ultimoErro = e;
                log("⚠️ Tentativa " + (i + 1) + "/" + maxTentativas + " falhou: " + e.getMessage());

                if (i < maxTentativas - 1) {
                    // Remove servidor problemático temporariamente
                    atualizarListaServidores();
                }
            }
        }

        throw new RemoteException("Falha após " + maxTentativas + " tentativas", ultimoErro);
    }

    // =========================================================================
    //  IMPLEMENTAÇÃO DA API RMI (GatewayService)
    // =========================================================================

    @Override
    public boolean login(String username, String password) throws RemoteException {
        log("📥 LOGIN: " + username);


        try {
            Boolean resultado = (Boolean) chamarComRetry(
                    "login",
                    new Object[]{username, password},
                    new Class[]{String.class, String.class},
                    3
            );
            return resultado != null && resultado;

        } catch (Exception e) {
            throw new RemoteException("Erro no login", e);
        }
    }

    @Override
    public List<String> listarArquivos() throws RemoteException {
        log("📥 LISTAR ARQUIVOS");

        try {
            return (List<String>) chamarComRetry(
                    "listarArquivos",
                    new Object[]{},
                    new Class[]{},
                    3
            );
        } catch (Exception e) {
            throw new RemoteException("Erro ao listar arquivos", e);
        }
    }

    @Override
    public boolean upload(String nomeArquivo, byte[] conteudo) throws RemoteException {
        log("📥 UPLOAD: " + nomeArquivo + " (" + conteudo.length + " bytes)");

        try {
            Boolean resultado = (Boolean) chamarComRetry(
                    "upload",
                    new Object[]{nomeArquivo, conteudo},
                    new Class[]{String.class, byte[].class},
                    3
            );
            return resultado != null && resultado;

        } catch (Exception e) {
            throw new RemoteException("Erro no upload", e);
        }
    }

    @Override
    public byte[] download(String nomeArquivo) throws RemoteException {
        log("📥 DOWNLOAD: " + nomeArquivo);

        try {
            return (byte[]) chamarComRetry(
                    "download",
                    new Object[]{nomeArquivo},
                    new Class[]{String.class},
                    3
            );
        } catch (Exception e) {
            throw new RemoteException("Erro no download", e);
        }
    }

    @Override
    public String getSistemaHash() throws RemoteException {
        log("📥 HASH GLOBAL");

        try {
            return (String) chamarComRetry(
                    "gerarHashGlobal",
                    new Object[]{},
                    new Class[]{},
                    3
            );
        } catch (Exception e) {
            throw new RemoteException("Erro ao gerar hash", e);
        }
    }

    @Override
    public boolean criarConta(String user, String pass) throws RemoteException {
        log("📥 CRIAR CONTA: " + user);

        try {
            Boolean resultado = (Boolean) chamarComRetry(
                    "salvarUsuario",
                    new Object[]{new Usuario(user, pass)},
                    new Class[]{Usuario.class},
                    3
            );
            return resultado != null && resultado;

        } catch (Exception e) {
            throw new RemoteException("Erro ao criar conta", e);
        }
    }

    // =========================================================================
    //  CALLBACKS JGROUPS
    // =========================================================================

    @Override
    public void viewAccepted(View view) {
        log("═══════════════════════════════════════════════");
        log("🔄 NOVA VIEW DO CLUSTER: " + view.size() + " membros");

        atualizarListaServidores();

        log("🖥️  SERVIDORES DISPONÍVEIS: " + servidoresAtivos.size());
        for (int i = 0; i < servidoresAtivos.size(); i++) {
            log("   [" + i + "] " + servidoresAtivos.get(i));
        }
        log("═══════════════════════════════════════════════");
    }

    private void atualizarListaServidores() {
        View view = canal.getView();
        servidoresAtivos.clear();

        // Adiciona todos exceto o próprio Gateway
        for (Address addr : view.getMembers()) {
            if (!addr.equals(canal.getAddress())) {
                servidoresAtivos.add(addr);
            }
        }

        // Reset Round-Robin
        roundRobinIndex.set(0);
        if (servidoresAtivos.isEmpty()) {
            log("⚠️  AVISO: Nenhum servidor backend disponível!");
        }
    }

    @Override
    public void receive(Message msg) {
        // Gateway não processa mensagens de replicação
    }

    @Override
    public void close() {
        log("Encerrando Gateway...");
        if (dispatcher != null) dispatcher.stop();
        if (canal != null) canal.close();
    }

    // =========================================================================
    //  MAIN
    // =========================================================================

    public static void main(String[] args) {
        try {
            System.out.println("╔════════════════════════════════════════════╗");
            System.out.println("║   GATEWAY COM LOAD BALANCER ROUND-ROBIN   ║");
            System.out.println("║   RMI (Cliente) + JGroups (Servidores)    ║");
            System.out.println("╚════════════════════════════════════════════╝");
            System.out.println();

            GatewayServer gateway = new GatewayServer();

            // Exporta como serviço RMI
            GatewayService stub =
                    (GatewayService) UnicastRemoteObject.exportObject(gateway, 0);

            // Registra no RMI Registry
            Registry registry = LocateRegistry.createRegistry(1099);
            registry.rebind("Service", stub);

            System.out.println("✅ Gateway RMI ativo na porta 1099");
            System.out.println("✅ Conectado ao cluster: " + CLUSTER);
            System.out.println("✅ Aguardando clientes...");
            System.out.println();

            // Mantém gateway ativo
            Thread.currentThread().join();

        } catch (Exception e) {
            System.err.println("❌ ERRO ao iniciar Gateway:");
            e.printStackTrace();
        }
    }
}