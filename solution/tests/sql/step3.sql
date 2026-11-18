-- Шаг 3 из 3: банк отозвал старый платёж, первый платёж пользователя с тремя и более оплатами.
-- Платёж старше окна хеша (30 дней): штатный прогон его не видит, находит audit.
-- После этого строка исчезает из витрины, а у остальных строк пользователя меняются
-- first_paid_at, когорта и purchase_number.
USE app;
UPDATE payments SET status = 'declined'
WHERE id = (SELECT id FROM (SELECT MIN(p.id) AS id FROM payments p
            JOIN users u ON u.id = p.user_id
            WHERE p.status = 'success' AND u.is_tester = 0 AND u.is_staff = 0
              AND (SELECT COUNT(*) FROM payments p2
                   WHERE p2.user_id = p.user_id AND p2.status = 'success') >= 3) t);
