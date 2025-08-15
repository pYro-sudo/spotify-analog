package by.losik.service;

import by.losik.entity.Sex;
import by.losik.entity.User;
import by.losik.repository.UserRepository;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.quarkus.cache.CacheInvalidate;
import io.quarkus.cache.CacheInvalidateAll;
import io.quarkus.cache.CacheResult;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.faulttolerance.CircuitBreaker;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.faulttolerance.Timeout;
import org.hibernate.service.spi.ServiceException;

import java.util.List;

@ApplicationScoped
@Slf4j
@Retry(maxRetries = 4, delay = 1000)
@Timeout(value = 30000)
@CircuitBreaker(
        requestVolumeThreshold = 4,
        failureRatio = 0.4,
        delay = 10
)
public class UserService {

    @Inject
    UserRepository userRepository;

    @Inject
    PrometheusMeterRegistry meterRegistry;

    @Transactional
    @CacheResult(cacheName = "users")
    public Uni<User> createUser(String name, int age, Sex gender, String email, String phone) {
        Timer.Sample timer = Timer.start(meterRegistry);
        User user = new User();
        user.setName(name);
        user.setAge(age);
        user.setGender(gender);
        user.setEmail(email);
        user.setPhone(phone);
        user.setVerificationTick(false);

        return userRepository.persist(user)
                .onItem().invoke(usr -> {
                    timer.stop(meterRegistry.timer("user.service.create.time", "gender", gender.name()));
                    meterRegistry.counter("user.service.operations", "operation", "create", "status", "success").increment();
                })
                .onFailure().invoke(throwable -> {
                    timer.stop(meterRegistry.timer("user.service.create.time", "gender", gender.name()));
                    meterRegistry.counter("user.service.operations", "operation", "create", "status", "failed").increment();
                    log.error("Failed to create user with email {}", email, throwable);
                })
                .onFailure().transform(throwable ->
                        new ServiceException("Failed to create user", throwable));
    }

    @Transactional
    @CacheResult(cacheName = "users")
    public Uni<User> getUserById(Long id) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return userRepository.findById(id)
                .onItem().invoke(usr -> {
                    timer.stop(meterRegistry.timer("user.service.get.by.id.time"));
                    meterRegistry.counter("user.service.operations", "operation", "getById", "status", "success").increment();
                    if (usr != null) {
                        meterRegistry.counter("user.service.cache", "cache", "users", "result", "hit").increment();
                    }
                })
                .onFailure().invoke(throwable -> {
                    timer.stop(meterRegistry.timer("user.service.get.by.id.time"));
                    meterRegistry.counter("user.service.operations", "operation", "getById", "status", "failed").increment();
                    meterRegistry.counter("user.service.cache", "cache", "users", "result", "miss").increment();
                    log.error("Failed to fetch user {}", id, throwable);
                });
    }

    @Transactional
    @CacheResult(cacheName = "users-list")
    public Uni<List<User>> getAllUsers() {
        Timer.Sample timer = Timer.start(meterRegistry);
        return userRepository.listAll()
                .onItem().invoke(users -> {
                    timer.stop(meterRegistry.timer("user.service.get.all.time"));
                    meterRegistry.counter("user.service.operations", "operation", "getAll", "status", "success").increment();
                    meterRegistry.gauge("user.service.count.all", users.size());
                })
                .onFailure().invoke(throwable -> {
                    timer.stop(meterRegistry.timer("user.service.get.all.time"));
                    meterRegistry.counter("user.service.operations", "operation", "getAll", "status", "failed").increment();
                    log.error("Failed to fetch all users", throwable);
                });
    }

    @Transactional
    @CacheResult(cacheName = "users-by-name")
    public Uni<List<User>> getUsersByName(String name) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return userRepository.findByName(name)
                .onItem().invoke(users -> {
                    timer.stop(meterRegistry.timer("user.service.get.by.name.time", "name", name));
                    meterRegistry.counter("user.service.operations", "operation", "getByName", "status", "success").increment();
                })
                .onFailure().invoke(throwable -> {
                    timer.stop(meterRegistry.timer("user.service.get.by.name.time", "name", name));
                    meterRegistry.counter("user.service.operations", "operation", "getByName", "status", "failed").increment();
                    log.error("Failed to fetch users by name {}", name, throwable);
                });
    }

    @Transactional
    @CacheResult(cacheName = "users-by-gender")
    public Uni<List<User>> getUsersByGender(Sex gender) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return userRepository.findByGender(gender)
                .onItem().invoke(users -> {
                    timer.stop(meterRegistry.timer("user.service.get.by.gender.time", "gender", gender.name()));
                    meterRegistry.counter("user.service.operations", "operation", "getByGender", "status", "success").increment();
                })
                .onFailure().invoke(throwable -> {
                    timer.stop(meterRegistry.timer("user.service.get.by.gender.time", "gender", gender.name()));
                    meterRegistry.counter("user.service.operations", "operation", "getByGender", "status", "failed").increment();
                    log.error("Failed to fetch users by gender {}", gender, throwable);
                });
    }

    @Transactional
    @CacheResult(cacheName = "users-by-age")
    public Uni<List<User>> getUsersByAgeRange(int minAge, int maxAge) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return userRepository.findByAgeRange(minAge, maxAge)
                .onItem().invoke(users -> {
                    timer.stop(meterRegistry.timer("user.service.get.by.age.time",
                            "minAge", String.valueOf(minAge), "maxAge", String.valueOf(maxAge)));
                    meterRegistry.counter("user.service.operations", "operation", "getByAge", "status", "success").increment();
                })
                .onFailure().invoke(throwable -> {
                    timer.stop(meterRegistry.timer("user.service.get.by.age.time",
                            "minAge", String.valueOf(minAge), "maxAge", String.valueOf(maxAge)));
                    meterRegistry.counter("user.service.operations", "operation", "getByAge", "status", "failed").increment();
                    log.error("Failed to fetch users by age range {}-{}", minAge, maxAge, throwable);
                });
    }

    @Transactional
    @CacheResult(cacheName = "users-by-email")
    public Uni<List<User>> getUsersByEmail(String email) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return userRepository.findByEmail(email)
                .onItem().invoke(users -> {
                    timer.stop(meterRegistry.timer("user.service.get.by.email.time", "email", email));
                    meterRegistry.counter("user.service.operations", "operation", "getByEmail", "status", "success").increment();
                })
                .onFailure().invoke(throwable -> {
                    timer.stop(meterRegistry.timer("user.service.get.by.email.time", "email", email));
                    meterRegistry.counter("user.service.operations", "operation", "getByEmail", "status", "failed").increment();
                    log.error("Failed to fetch users by email {}", email, throwable);
                });
    }

    @Transactional
    @CacheResult(cacheName = "users-by-phone")
    public Uni<List<User>> getUsersByPhone(String phone) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return userRepository.findByPhone(phone)
                .onItem().invoke(users -> {
                    timer.stop(meterRegistry.timer("user.service.get.by.phone.time", "phone", phone));
                    meterRegistry.counter("user.service.operations", "operation", "getByPhone", "status", "success").increment();
                })
                .onFailure().invoke(throwable -> {
                    timer.stop(meterRegistry.timer("user.service.get.by.phone.time", "phone", phone));
                    meterRegistry.counter("user.service.operations", "operation", "getByPhone", "status", "failed").increment();
                    log.error("Failed to fetch users by phone {}", phone, throwable);
                });
    }

    @Transactional
    @CacheInvalidate(cacheName = "users")
    @CacheInvalidate(cacheName = "users-list")
    @CacheInvalidate(cacheName = "users-by-gender")
    @CacheInvalidate(cacheName = "users-by-age")
    @CacheInvalidate(cacheName = "users-by-name")
    @CacheInvalidate(cacheName = "users-by-email")
    @CacheInvalidate(cacheName = "users-by-phone")
    public Uni<User> updateUserEmail(Long id, String newEmail) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return userRepository.updateEmail(id, newEmail)
                .onItem().invoke(usr -> {
                    timer.stop(meterRegistry.timer("user.service.update.email.time"));
                    meterRegistry.counter("user.service.operations", "operation", "updateEmail", "status", "success").increment();
                    meterRegistry.counter("user.service.cache.invalidations").increment();
                })
                .onFailure().invoke(throwable -> {
                    timer.stop(meterRegistry.timer("user.service.update.email.time"));
                    meterRegistry.counter("user.service.operations", "operation", "updateEmail", "status", "failed").increment();
                    log.error("Failed to update email for user {}", id, throwable);
                });
    }

    @Transactional
    @CacheInvalidate(cacheName = "users")
    @CacheInvalidate(cacheName = "users-list")
    @CacheInvalidate(cacheName = "users-by-gender")
    @CacheInvalidate(cacheName = "users-by-age")
    @CacheInvalidate(cacheName = "users-by-name")
    @CacheInvalidate(cacheName = "users-by-email")
    @CacheInvalidate(cacheName = "users-by-phone")
    public Uni<User> verifyUser(Long id, boolean verified) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return userRepository.updateVerificationStatus(id, verified)
                .onItem().invoke(usr -> {
                    timer.stop(meterRegistry.timer("user.service.verify.time", "verified", String.valueOf(verified)));
                    meterRegistry.counter("user.service.operations", "operation", "verify", "status", "success").increment();
                    meterRegistry.counter("user.service.cache.invalidations").increment();
                })
                .onFailure().invoke(throwable -> {
                    timer.stop(meterRegistry.timer("user.service.verify.time", "verified", String.valueOf(verified)));
                    meterRegistry.counter("user.service.operations", "operation", "verify", "status", "failed").increment();
                    log.error("Failed to verify user {}", id, throwable);
                });
    }

    @Transactional
    @CacheInvalidate(cacheName = "users")
    @CacheInvalidate(cacheName = "users-list")
    @CacheInvalidate(cacheName = "users-by-gender")
    @CacheInvalidate(cacheName = "users-by-age")
    @CacheInvalidate(cacheName = "users-by-name")
    @CacheInvalidate(cacheName = "users-by-email")
    @CacheInvalidate(cacheName = "users-by-phone")
    public Uni<Boolean> deleteUser(Long id) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return userRepository.deleteById(id)
                .onItem().invoke(deleted -> {
                    timer.stop(meterRegistry.timer("user.service.delete.time"));
                    meterRegistry.counter("user.service.operations", "operation", "delete", "status", "success").increment();
                    meterRegistry.counter("user.service.cache.invalidations").increment();
                    if (deleted) {
                        meterRegistry.counter("user.service.deleted").increment();
                    }
                })
                .onFailure().invoke(throwable -> {
                    timer.stop(meterRegistry.timer("user.service.delete.time"));
                    meterRegistry.counter("user.service.operations", "operation", "delete", "status", "failed").increment();
                    log.error("Failed to delete user {}", id, throwable);
                });
    }

    @Transactional
    @CacheResult(cacheName = "users-by-verification")
    public Uni<List<User>> getUsersByVerificationStatus(boolean verified) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return userRepository.findByVerificationStatus(verified)
                .onItem().invoke(users -> {
                    timer.stop(meterRegistry.timer("user.service.get.by.verification.time", "verified", String.valueOf(verified)));
                    meterRegistry.counter("user.service.operations", "operation", "getByVerification", "status", "success").increment();
                })
                .onFailure().invoke(throwable -> {
                    timer.stop(meterRegistry.timer("user.service.get.by.verification.time", "verified", String.valueOf(verified)));
                    meterRegistry.counter("user.service.operations", "operation", "getByVerification", "status", "failed").increment();
                    log.error("Failed to fetch users by verification status {}", verified, throwable);
                });
    }

    @Transactional
    @CacheInvalidate(cacheName = "users")
    @CacheInvalidate(cacheName = "users-list")
    @CacheInvalidate(cacheName = "users-by-gender")
    @CacheInvalidate(cacheName = "users-by-age")
    @CacheInvalidate(cacheName = "users-by-name")
    @CacheInvalidate(cacheName = "users-by-email")
    @CacheInvalidate(cacheName = "users-by-phone")
    @CacheInvalidate(cacheName = "users-by-verification")
    public Uni<Long> deleteUsersBelowAge(int maxAge) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return userRepository.deleteUsersByAge(maxAge)
                .onItem().invoke(count -> {
                    timer.stop(meterRegistry.timer("user.service.delete.by.age.time", "maxAge", String.valueOf(maxAge)));
                    meterRegistry.counter("user.service.operations", "operation", "deleteByAge", "status", "success").increment();
                    meterRegistry.counter("user.service.cache.invalidations").increment();
                    meterRegistry.counter("user.service.deleted.by.age", "count", String.valueOf(count)).increment(count);
                })
                .onFailure().invoke(throwable -> {
                    timer.stop(meterRegistry.timer("user.service.delete.by.age.time", "maxAge", String.valueOf(maxAge)));
                    meterRegistry.counter("user.service.operations", "operation", "deleteByAge", "status", "failed").increment();
                    log.error("Failed to delete users below age {}", maxAge, throwable);
                });
    }

    @Transactional
    @Scheduled(every = "10m")
    @CacheInvalidateAll(cacheName = "users")
    @CacheInvalidateAll(cacheName = "users-list")
    @CacheInvalidateAll(cacheName = "users-by-gender")
    @CacheInvalidateAll(cacheName = "users-by-age")
    @CacheInvalidateAll(cacheName = "users-by-name")
    @CacheInvalidateAll(cacheName = "users-by-email")
    @CacheInvalidateAll(cacheName = "users-by-phone")
    @CacheInvalidateAll(cacheName = "users-by-verification")
    public Uni<Void> purgeUserCache() {
        Timer.Sample timer = Timer.start(meterRegistry);
        return Uni.createFrom().voidItem()
                .onItem().invoke(() -> {
                    timer.stop(meterRegistry.timer("user.service.purge.time"));
                    meterRegistry.counter("user.service.cache.invalidations.all").increment();
                    log.info("User cache purge completed!");
                })
                .onFailure().invoke(item -> {
                    timer.stop(meterRegistry.timer("user.service.purge.time"));
                    log.error("Exception occurred during user cache purge", item);
                });
    }
}