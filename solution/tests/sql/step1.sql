-- Сценарии из описания источника, которых нет в шести пакетах стенда. Шаг 1 из 3.
-- Применяется только на черновом стенде тестами test_scenarios.py, после них make reset.
-- Правило «MariaDB только читайте» относится к пайплайну, а не к тестам стенда.
USE app;

-- Снят флаг тестового: заказы пользователя должны вернуться в витрину.
UPDATE users SET is_tester = 0
WHERE id = (SELECT id FROM (SELECT MIN(id) AS id FROM users WHERE is_tester = 1) t);

-- Пользователь с заказами стал сотрудником: его заказы должны исчезнуть.
UPDATE users SET is_staff = 1
WHERE id = (SELECT user_id FROM (SELECT user_id FROM orders GROUP BY user_id
                                 ORDER BY COUNT(*) DESC, user_id LIMIT 1) t);

-- План сменил категорию: все его заказы получают новую plan_category.
UPDATE plans SET size_gb = 20.0
WHERE id = (SELECT id FROM (SELECT MIN(id) AS id FROM plans
                            WHERE is_unlimited = 0 AND size_gb > 1 AND size_gb <= 10) t);

-- Банк отозвал свежий платёж: success -> declined без участия pending.
-- Ловит хеш статуса в сверке каждого прогона (платёж моложе 30 дней).
UPDATE payments SET status = 'declined'
WHERE id = (SELECT id FROM (SELECT MAX(p.id) AS id FROM payments p
                            JOIN users u ON u.id = p.user_id
                            JOIN orders o ON o.payment_uid = p.payment_uid
                            WHERE p.status = 'success' AND u.is_tester = 0 AND u.is_staff = 0) t);

-- Опоздавший платёж: пришёл pending с датой на 12 дней раньше соседних, и заказ под него.
-- payment_uid помечен префиксом eeeeeeee- той же длины, что UUID, чтобы найти его на шаге 2.
-- В витрину заказ попадёт только после подтверждения на шаге 2.
INSERT INTO payments (payment_uid, user_id, payment_system_id, status, amount_eur, created_at)
SELECT CONCAT('eeeeeeee', SUBSTRING(UUID(), 9)), user_id, 1, 'pending', 9.99, DATE_SUB(created_at, INTERVAL 12 DAY)
FROM orders WHERE payment_uid IS NULL ORDER BY id DESC LIMIT 1;
INSERT INTO orders (user_id, payment_uid, plan_id, points_used, platform, created_at)
SELECT user_id, payment_uid, 1, 0, 'web', created_at
FROM payments WHERE payment_uid LIKE 'eeeeeeee-%' ORDER BY id DESC LIMIT 1;
