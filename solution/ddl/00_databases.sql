-- Слои хранилища. Каждый слой в своей базе: так проще раздавать права.
CREATE DATABASE IF NOT EXISTS meta COMMENT 'служебные таблицы пайплайна: состояние загрузок, результаты проверок';
CREATE DATABASE IF NOT EXISTS raw  COMMENT 'копии таблиц источника как есть, с версией и признаком удаления';
CREATE DATABASE IF NOT EXISTS stg  COMMENT 'очищенные таблицы: одна актуальная версия строки, без удалённых';
CREATE DATABASE IF NOT EXISTS mart COMMENT 'витрины для аналитиков и BI';
