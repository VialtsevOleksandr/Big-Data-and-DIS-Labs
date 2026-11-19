-- Конфігурація B: кожен лідер публікує зміни таблиці notes.
-- Публікація передає INSERT, UPDATE, DELETE і TRUNCATE; DDL і стан послідовностей не передаються.
-- Рядок для UPDATE/DELETE на іншому вузлі шукається за REPLICA IDENTITY, тобто за первинним ключем id.
CREATE PUBLICATION notes_pub FOR TABLE notes;
