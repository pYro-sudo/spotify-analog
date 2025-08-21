package by.losik.entity;

import jakarta.persistence.*;
import jakarta.validation.constraints.*;
import lombok.Data;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Entity
@Table(name = "user", schema = "users")
@Data
@Schema(description = "User entity representing a system user")
public class User {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE)
    @Column(name = "id", columnDefinition = "serial")
    @Schema(description = "Unique identifier of the user", example = "1")
    private Long id;

    @Size(min = 0, max = 50)
    @Column(name = "name", columnDefinition = "varchar")
    @Schema(description = "User's full name", example = "John Doe", maxLength = 50)
    private String name;

    @Min(value = 0)
    @Max(value = 200)
    @Column(name = "age", columnDefinition = "integer")
    @Schema(description = "User's age in years", example = "25", minimum = "0", maximum = "200")
    private int age;

    @Column(name = "gender", columnDefinition = "varchar")
    @Schema(description = "User's gender", example = "MALE", implementation = Sex.class)
    private Sex gender;

    @Email
    @Column(name = "email", columnDefinition = "varchar")
    @Schema(description = "User's email address", example = "john.doe@example.com", format = "email")
    private String email;

    @Pattern(regexp = "^\\+[0-9]{9,15}$")
    @Column(name = "phone", columnDefinition = "varchar")
    @Schema(description = "User's phone number in E.164 format", example = "+375291234567", pattern = "^\\+[0-9]{9,15}$")
    private String phone;

    @Column(name = "verification_tick", columnDefinition = "varchar")
    @Schema(description = "Indicates if the user account is verified", example = "true")
    private boolean verificationTick;
}