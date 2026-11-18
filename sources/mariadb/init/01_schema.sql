-- Сервис пакетов: OLTP-база (MariaDB). Все DATETIME — в UTC.
CREATE DATABASE IF NOT EXISTS app CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE app;

CREATE TABLE users (
    id          INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    country     CHAR(2)      NOT NULL,
    is_tester   TINYINT(1)   NOT NULL DEFAULT 0,
    is_staff    TINYINT(1)   NOT NULL DEFAULT 0,
    created_at  DATETIME     NOT NULL
) ENGINE=InnoDB;

CREATE TABLE plans (
    id            INT UNSIGNED NOT NULL PRIMARY KEY,
    name          VARCHAR(64)  NOT NULL,
    size_gb       DECIMAL(6,1) NULL COMMENT 'NULL для безлимитных',
    days          INT          NOT NULL,
    is_unlimited  TINYINT(1)   NOT NULL DEFAULT 0,
    price_eur     DECIMAL(10,2) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE payment_systems (
    id    INT UNSIGNED NOT NULL PRIMARY KEY,
    name  VARCHAR(32)  NOT NULL
) ENGINE=InnoDB;

CREATE TABLE payments (
    id                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    payment_uid        CHAR(36)     NOT NULL,
    user_id            INT UNSIGNED NOT NULL,
    payment_system_id  INT UNSIGNED NOT NULL,
    status             ENUM('pending','success','declined') NOT NULL,
    amount_eur         DECIMAL(10,2) NOT NULL,
    created_at         DATETIME     NOT NULL COMMENT 'время платежа у платёжной системы',
    UNIQUE KEY uk_payment_uid (payment_uid),
    KEY idx_payments_user (user_id),
    KEY idx_payments_created (created_at)
) ENGINE=InnoDB;

CREATE TABLE orders (
    id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id      INT UNSIGNED NOT NULL,
    payment_uid  CHAR(36)     NULL COMMENT 'NULL — заказ оплачен баллами',
    plan_id      INT UNSIGNED NOT NULL,
    points_used  INT          NOT NULL DEFAULT 0,
    platform     ENUM('web','ios','android') NOT NULL,
    created_at   DATETIME     NOT NULL,
    KEY idx_orders_user (user_id),
    KEY idx_orders_payment (payment_uid)
) ENGINE=InnoDB;

CREATE TABLE activations (
    id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    order_id      BIGINT UNSIGNED NOT NULL,
    user_id       INT UNSIGNED NOT NULL,
    activated_at  DATETIME     NOT NULL,
    KEY idx_act_order (order_id)
) ENGINE=InnoDB;

CREATE TABLE usage_daily (
    id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    activation_id  BIGINT UNSIGNED NOT NULL,
    usage_date     DATE         NOT NULL,
    usage_units    INT          NOT NULL,
    cost_eur       DECIMAL(10,2) NOT NULL COMMENT 'себестоимость расхода за день',
    KEY idx_usage_act (activation_id),
    KEY idx_usage_date (usage_date)
) ENGINE=InnoDB;

CREATE TABLE refunds (
    id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    payment_uid  CHAR(36)     NOT NULL,
    amount_eur   DECIMAL(10,2) NOT NULL,
    created_at   DATETIME     NOT NULL,
    KEY idx_refunds_payment (payment_uid)
) ENGINE=InnoDB;
