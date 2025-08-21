package by.losik.repository;

import by.losik.entity.Sex;
import by.losik.entity.User;
import io.quarkus.hibernate.reactive.panache.PanacheRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;

import java.time.Duration;
import java.util.List;

@ApplicationScoped
public class UserRepository implements PanacheRepository<User> {

    @Operation(summary = "Find users by name", description = "Retrieves all users with the specified name")
    public Uni<List<User>> findByName(
            @Parameter(description = "Name to search for", example = "John", required = true)
            String name) {
        return list("name", name);
    }

    @Operation(summary = "Find users by age range", description = "Retrieves users within the specified age range (inclusive)")
    public Uni<List<User>> findByAgeRange(
            @Parameter(description = "Minimum age", example = "18", required = true)
            int minAge,
            @Parameter(description = "Maximum age", example = "65", required = true)
            int maxAge) {
        return list("age between ?1 and ?2", minAge, maxAge);
    }

    @Operation(summary = "Find users by gender", description = "Retrieves users with the specified gender")
    public Uni<List<User>> findByGender(
            @Parameter(description = "Gender to filter by", example = "MALE", required = true)
            Sex gender) {
        return list("gender", gender);
    }

    @Operation(summary = "Find users by email", description = "Retrieves users with the specified email address")
    public Uni<List<User>> findByEmail(
            @Parameter(description = "Email address to search for", example = "john.doe@example.com", required = true)
            String email) {
        return list("email", email);
    }

    @Operation(summary = "Find users by phone number", description = "Retrieves users with the specified phone number")
    public Uni<List<User>> findByPhone(
            @Parameter(description = "Phone number in E.164 format", example = "+375291234567", required = true)
            String phone) {
        return list("phone", phone);
    }

    @Operation(summary = "Find users by verification status", description = "Retrieves users based on their verification status")
    public Uni<List<User>> findByVerificationStatus(
            @Parameter(description = "Verification status filter", example = "true", required = true)
            boolean verified) {
        return list("verificationTick", verified);
    }

    @Operation(summary = "Update user email", description = "Updates the email address of a specific user")
    public Uni<User> updateEmail(
            @Parameter(description = "User ID", example = "1", required = true)
            Long id,
            @Parameter(description = "New email address", example = "new.email@example.com", required = true)
            String newEmail) {
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

    @Operation(summary = "Update verification status", description = "Updates the verification status of a specific user")
    public Uni<User> updateVerificationStatus(
            @Parameter(description = "User ID", example = "1", required = true)
            Long id,
            @Parameter(description = "New verification status", example = "true", required = true)
            boolean verified) {
        return findById(id)
                .onItem().transformToUni(user -> {
                    if (user != null) {
                        user.setVerificationTick(verified);
                        return persist(user);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    @Operation(summary = "Delete users by maximum age", description = "Deletes all users with age less than or equal to the specified value")
    public Uni<Long> deleteUsersByAge(
            @Parameter(description = "Maximum age for deletion", example = "18", required = true)
            int maxAge) {
        return delete("age <= ?1", maxAge);
    }
}