package database;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;

import java.util.HashMap;
import java.util.Map;

public class JPAUtil {

    private static EntityManagerFactory emf;

    public static void init(String nomeBanco) {
        if (emf != null) {
            emf.close(); // fecha se já existe (ex: testes)
        }

        Map<String, String> props = new HashMap<>();

        // Sobrescreve o arquivo do SQLite no persistence.xml
        props.put("jakarta.persistence.jdbc.url",
                "jdbc:sqlite:" + nomeBanco);

        emf = Persistence.createEntityManagerFactory("FileServerPU", props);
    }

    public static EntityManager getEntityManager() {
        if (emf == null) {
            throw new IllegalStateException("JPAUtil.init(nomeBanco) não foi chamado!");
        }
        return emf.createEntityManager();
    }
}
