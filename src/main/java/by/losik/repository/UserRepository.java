package by.losik.repository;

import by.losik.entity.Sex;
import by.losik.entity.User;
import io.quarkus.hibernate.reactive.panache.PanacheRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

import java.time.Duration;
import java.util.List;

@ApplicationScoped
public class UserRepository implements PanacheRepository<User> {

    public Uni<List<User>> findByName(String name) {
        return list("name", name);
    }

    public Uni<List<User>> findByAgeRange(int minAge, int maxAge) {
        return list("age between ?1 and ?2", minAge, maxAge);
    }

    public Uni<List<User>> findByGender(Sex gender) {
        return list("gender", gender);
    }

    public Uni<List<User>> findByEmail(String email) {
        return list("email", email);
    }

    public Uni<List<User>> findByPhone(String phone) {
        return list("phone", phone);
    }

    public Uni<List<User>> findByVerificationStatus(boolean verified) {
        return list("verificationTick", verified);
    }

    public Uni<User> updateEmail(Long id, String newEmail) {
        return findById(id)
                .onItem().transformToUni(user -> {
                    if (user != null) {
                        user.setEmail(newEmail);
                        return persist(user);
                    }
                    return Uni.createFrom().nullItem();
                }).ifNoItem().after(Duration.ofMillis(1000)).fail()
                .onFailure().recoverWithNull();
    }

    public Uni<User> updateVerificationStatus(Long id, boolean verified) {
        return findById(id)
                .onItem().transformToUni(user -> {
                    if (user != null) {
                        user.setVerificationTick(verified);
                        return persist(user);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<Long> deleteUsersByAge(int maxAge) {
        return delete("age <= ?1", maxAge);
    }
}