-- SQLITE BOT SCHEMA
BEGIN;

CREATE TABLE Users (
    discordId INTEGER NOT NULL,
    unixUser TEXT NOT NULL,
    CONSTRAINT Users_uq UNIQUE (DiscordId, UnixUser)
);

CREATE TABLE StatusMessages (
    channelId INTEGER NOT NULL,
    messageId INTEGER NOT NULL PRIMARY KEY
);

COMMIT;