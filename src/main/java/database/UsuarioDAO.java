package database;

import jakarta.persistence.*;
import model.Usuario;
import java.util.List;

public class UsuarioDAO {

    public boolean salvar(Usuario usuario) {
        EntityManager em = JPAUtil.getEntityManager();
        try {
            em.getTransaction().begin();
            em.persist(usuario);
            em.getTransaction().commit();
            return true;
        } catch (Exception e) {
            return false;
        } finally {
            em.close();
        }
    }

    public boolean replicarUsuario(Usuario usuario) {
        EntityManager em = JPAUtil.getEntityManager();
        try {
            System.out.println("[DadosServer] 🔄 Replicando usuário: " + usuario.getUsername() + " (ID: " + usuario.getId() + ")");

            // Verifica se já existe
            Usuario existente = buscarPorUsername(usuario.getUsername());
            if (existente != null) {
                System.out.println("[DadosServer] ⚠️ Usuário já existe, pulando replicação: " + usuario.getUsername());
                return true; // Retorna true pois não é um erro
            }

            em.getTransaction().begin();

            // ✅ merge() para objetos que já têm ID (vindos de replicação)
            em.merge(usuario);

            em.getTransaction().commit();

            System.out.println("[DadosServer] ✅ Usuário replicado com sucesso: " + usuario.getUsername());
            return true;

        } catch (Exception e) {
            System.err.println("[DadosServer] ❌ Erro ao replicar usuário: " + e.getMessage());
            e.printStackTrace();

            if (em.getTransaction().isActive()) {
                em.getTransaction().rollback();
            }
            return false;

        } finally {
            em.close();
        }
    }

    public Usuario buscarPorUsername(String username) {
        EntityManager em = JPAUtil.getEntityManager();
        try {
            TypedQuery<Usuario> q = em.createQuery(
                    "SELECT u FROM Usuario u WHERE u.username = :username",
                    Usuario.class
            );
            q.setParameter("username", username);
            return q.getSingleResult();
        } catch (NoResultException e) {
            return null;
        } finally {
            em.close();
        }
    }

    public List<Usuario> listarTodos() {
        EntityManager em = JPAUtil.getEntityManager();
        try {
            return em.createQuery("FROM Usuario", Usuario.class).getResultList();
        } finally {
            em.close();
        }
    }
}
