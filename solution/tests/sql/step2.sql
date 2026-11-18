-- Шаг 2 из 3: опоздавший платёж из шага 1 подтвердился.
USE app;
UPDATE payments SET status = 'success' WHERE payment_uid LIKE 'eeeeeeee-%';
