# Restic Repository Browser

This project provides a web-based read-only interface to browse a [restic](https://restic.readthedocs.io) backup repostory.

## Goals

 * Provide an alternative implementation for restic internals
 * Provide analytics to understand how restic spends storage and how to free space

## Non-goals
 * Be a full-fledged alternative for accessing a restic repositories (use the official tool instead)

## Security implications

Restic repositories are fully encrypted and the official restic application is careful to avoid sharing too
much information.

This project also tries to avoid storing unencrypted data naively but does not guarantee it to the
same degree.

In particular:
 * A more efficient binary index of pack files (and in future other data) is built and kept in the cache
   directory unencrypted for random access. The index stores no user data but reveals hashes of files in the
   repository. With the index alone, some queries of "Does the repository contain a file with a certain hash?"
   can be answered.
 * The master key for the repository is kept in memory (same as restic itself).
 * restic code itself has undergone some scrutinee while this is third-party project.

## Usage

### From Docker

You need to mount 

```
docker run --rm -ti -p 8080:8080 -v <repository>:/repo:ro -v $HOME/.cache/restic:/restic-cache:ro jrudolph/restic-reader
```

Then open to https://localhost:8080.

### With Plain Java

You need at least Java 11.

Download a release jar and run it like this:

```
java -Drestic.repository=/opt/my-backup-repo -jar restic-reader.jar
```

Then open to https://localhost:8080.

### Settings

Settings can be specified on the command line e.g. with `-Drestic.repository=...`. Those have to go before `-jar ...`.
Some settings can also be specified using environment variables.

 * (required) setting `restic.repository` or env `RESTIC_REPOSITORY`: path to a restic repository, does not support
   the restic remote repository syntax.
 * (optional) setting `restic.password-file` or env `RESTIC_PASSWORD_FILE`: path to a file containing the plaintext password
   (if not specified the password will be prompted).
 * (optional) setting `restic.user-cache-dir`: path to the cache owned by restic. Defaults to `~/.cache/restic`.
 * (optional) setting `restic.cache-dir`: path to a directory where this tool can cache additional files. Defaults
   to `./restic-cache`.

## Operation

This tool will only read from the repository and the cache owned by restic. Repository files needed during operation will
be copied to the cache directory pointed to by `restic.cache-dir`.

### Indexing

Upon first use, the tool will create index files into the cache directory for quick random access to data
to avoid excessive RAM usage. Currently, indices might get stale when new data is added to the repository
in which case some operations may throw errors or report 404 on the web interface. Try deleting stale cache
files and restart the app.

### Remote repositories

This tool currently cannot access remote repositories directly. Instead, you can use `rclone` to mount remote
repositories locally (read-only mounts are recommended). Data files are never accessed directly but will copied
to the cache first.

## Disclaimer

This tool is provided as is. Be careful when operating on your backups. Bugs or misuse could accidentally damage
your data.