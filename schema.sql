-- SQLITE BOT SCHEMA

CREATE TABLE Users (
    discordId INTEGER NOT NULL,
    unixUser TEXT NOT NULL,
    CONSTRAINT Users_uq UNIQUE (DiscordId, UnixUser)
);