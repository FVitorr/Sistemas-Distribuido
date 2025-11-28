package database;

import jakarta.persistence.*;

public class JPAUtil {

    private static EntityManagerFactory emf =
            Persistence.createEntityManagerFactory("FileServerPU");

    public static EntityManager getEntityManager() {
        return emf.createEntityManager();
    }

    // For testing purposes
    public static void setEntityManagerFactory(EntityManagerFactory factory) {
        emf = factory;
    }

    public static EntityManagerFactory getEntityManagerFactory() {
        return emf;
    }
}
