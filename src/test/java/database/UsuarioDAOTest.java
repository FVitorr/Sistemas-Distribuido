package database;

import model.Usuario;
import org.junit.jupiter.api.*;
import jakarta.persistence.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class UsuarioDAOTest {

    private UsuarioDAO usuarioDAO;

    @BeforeAll
    void setupEntityManagerFactory() {
        // Configurar EntityManagerFactory para banco H2 em memória
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("FileServerPU");
        JPAUtil.setEntityManagerFactory(emf);
    }

    @BeforeEach
    void init() {
        usuarioDAO = new UsuarioDAO();
    }

    @Test
    void testSalvarEBuscarUsuario() {
        String username = "Dudinha" + System.currentTimeMillis();
        Usuario u = new Usuario(username, "1234");

        boolean saved = usuarioDAO.salvar(u);
        assertTrue(saved, "Usuário deve ser salvo com sucesso");

        Usuario encontrado = usuarioDAO.buscarPorUsername("teste");
        assertNotNull(encontrado, "Usuário deve ser encontrado");
        assertEquals("1234", encontrado.getPassword());
    }

    @Test
    void testBuscarUsuarioInexistente() {
        Usuario u = usuarioDAO.buscarPorUsername("naoexiste");
        assertNull(u, "Usuário inexistente deve retornar null");
    }

    @Test
    void testListarTodos() {
        usuarioDAO.salvar(new Usuario("usuario1", "pass1"));
        usuarioDAO.salvar(new Usuario("usuario2", "pass2"));

        List<Usuario> usuarios = usuarioDAO.listarTodos();
        assertTrue(usuarios.size() >= 2, "Deve retornar pelo menos 2 usuários");
    }

    @AfterAll
    void tearDown() {
        EntityManagerFactory emf = JPAUtil.getEntityManagerFactory();
        if (emf != null) {
            emf.close();
        }
    }
}
