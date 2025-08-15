package by.losik.entity;

import jakarta.persistence.*;
import jakarta.validation.constraints.*;
import lombok.Data;

@Entity
@Table(name = "user", schema = "users")
@Data
public class User {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE)
    @Column(name = "id", columnDefinition = "serial")
    private Long id;

    @Size(min = 0, max = 50)
    @Column(name = "name", columnDefinition = "varchar")
    private String name;

    @Min(value = 0)
    @Max(value = 200)
    @Column(name = "age", columnDefinition = "integer")
    private int age;

    @Column(name = "gender", columnDefinition = "varchar")
    private Sex gender;

    @Email
    @Column(name = "email", columnDefinition = "varchar")
    private String email;

    @Pattern(regexp = "^\\+[0-9]{9,15}$")
    @Column(name = "phone", columnDefinition = "varchar")
    private String phone;

    @Column(name = "verification_tick", columnDefinition = "varchar")
    private boolean verificationTick;
}