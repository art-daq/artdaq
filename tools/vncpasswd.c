/*
 * vncpasswd - set or update a user's VNC password file
 *
 * Writes the bit-reversed representation of a password (up to 8 characters)
 * to a password file compatible with the TigerVNC ~/.vnc/passwd format.
 * If the existing file contains a read-only password (bytes 8-15) it is
 * preserved unchanged.
 *
 * Usage: vncpasswd [-f <file>]
 *        vncpasswd -h
 *
 * Options:
 *   -f <file>   Write to <file> instead of the default location determined
 *               by the configuration hierarchy.  A leading "~/" is expanded
 *               to the current user's home directory.
 *   -h          Print usage and exit.
 *
 * Configuration hierarchy (later entries override earlier ones):
 *   1. Compiled-in default:       ~/.vnc/passwd
 *   2. System default config:     /usr/share/vnc/vncpasswd.conf
 *   3. System admin config:       /etc/vnc/vncpasswd.conf
 *   4. User config:               ~/.config/vnc/vncpasswd.conf
 *   5. -f command-line flag       (highest priority)
 *
 * When stdin is a terminal the user is prompted twice and the two entries
 * must match.  When stdin is not a terminal the password is read as a single
 * line from stdin (no confirmation prompt) to support non-interactive use.
 *
 * The output file is created with mode 0600.  Existing permissions on an
 * already-present file are corrected to 0600.  The write is performed
 * atomically: a temporary file in the same directory is written first and
 * then renamed into place.
 *
 * This program does not use elevated privileges.
 */

/* Enable POSIX and Linux extensions (O_NOFOLLOW, O_CLOEXEC, explicit_bzero, etc.) */
#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <termios.h>
#include <unistd.h>

#include "vnc_config.h"

#define VNC_PASSWORD_LEN 8
/* TigerVNC stores up to 16 bytes: 8 read-write + optional 8 read-only */
#define VNC_FILE_LEN 16
/* Maximum input length (extra byte for the '\n') */
#define MAX_INPUT 256

/* Reverse the bits in a single byte, matching the TigerVNC obfuscation */
static unsigned char reverse_bits(unsigned char byte)
{
unsigned char result = 0;
int i;
for (i = 0; i < 8; i++) {
if (byte & (1u << i)) {
result |= (unsigned char)(1u << (7 - i));
}
}
return result;
}

/* Print usage to stderr */
static void usage(const char* prog)
{
fprintf(stderr, "Usage: %s [-f <file>]\n", prog);
fprintf(stderr, "       %s -h\n", prog);
fprintf(stderr, "\nOptions:\n");
fprintf(stderr, "  -f <file>  Write to <file> instead of ~/.vnc/passwd\n");
fprintf(stderr, "             (overrides all config files)\n");
fprintf(stderr, "  -h         Show this help\n");
}

/*
 * Read a password from the terminal (no echo).
 * Writes up to buf_len-1 characters into buf and NUL-terminates.
 * Returns 0 on success, -1 on error.
 */
static int read_password_tty(const char* prompt, char* buf, size_t buf_len)
{
struct termios old_tc, new_tc;
FILE* tty;
size_t len;

tty = fopen("/dev/tty", "r+");
if (tty == NULL) {
perror("vncpasswd: fopen /dev/tty");
return -1;
}

/* Disable echo */
if (tcgetattr(fileno(tty), &old_tc) != 0) {
perror("vncpasswd: tcgetattr");
fclose(tty);
return -1;
}
new_tc = old_tc;
new_tc.c_lflag &= ~(tcflag_t)(ECHO | ECHOE | ECHOK | ECHONL);
if (tcsetattr(fileno(tty), TCSAFLUSH, &new_tc) != 0) {
perror("vncpasswd: tcsetattr");
fclose(tty);
return -1;
}

fprintf(tty, "%s", prompt);
fflush(tty);

if (fgets(buf, (int)buf_len, tty) == NULL) {
(void)tcsetattr(fileno(tty), TCSAFLUSH, &old_tc);
fprintf(tty, "\n");
fclose(tty);
return -1;
}

/* Restore echo before any output */
(void)tcsetattr(fileno(tty), TCSAFLUSH, &old_tc);
fprintf(tty, "\n");
fclose(tty);

/* Strip trailing newline */
len = strlen(buf);
if (len > 0 && buf[len - 1] == '\n') {
buf[len - 1] = '\0';
}

return 0;
}

/*
 * Read a password from stdin (non-interactive).
 * Strips a trailing newline if present.
 * Returns 0 on success, -1 on error.
 */
static int read_password_stdin(char* buf, size_t buf_len)
{
size_t len;

if (fgets(buf, (int)buf_len, stdin) == NULL) {
return -1;
}
len = strlen(buf);
if (len > 0 && buf[len - 1] == '\n') {
buf[len - 1] = '\0';
}
return 0;
}

/*
 * Ensure the directory containing filepath exists with mode 0700.
 * Creates the final directory component (one level) if absent.
 * Returns 0 on success, -1 on error.
 */
static int ensure_dir(const char* filepath)
{
char* dir;
char* slash;
int rc = 0;

dir = strdup(filepath);
if (dir == NULL) {
return -1;
}
slash = strrchr(dir, '/');
if (slash == NULL || slash == dir) {
free(dir);
return 0;
}
*slash = '\0';

if (mkdir(dir, 0700) != 0 && errno != EEXIST) {
fprintf(stderr, "vncpasswd: mkdir %s: %s\n", dir, strerror(errno));
rc = -1;
}
free(dir);
return rc;
}

/*
 * Read the existing password file (if any) to capture a read-only password.
 * The read-only password occupies bytes 8-15.  Sets *has_readonly = 1 and
 * copies the 8 bytes into readonly_buf if found; otherwise *has_readonly = 0.
 *
 * TOCTOU protections:
 *   O_NOFOLLOW: refuses to follow a symbolic link in the final path component.
 *   fstat: verifies the file is a regular file owned by the current user.
 */
static void read_existing(const char* path, unsigned char* readonly_buf, int* has_readonly)
{
int fd;
struct stat st;
unsigned char tmp[VNC_FILE_LEN];
ssize_t n;

*has_readonly = 0;

fd = open(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
if (fd < 0) {
return;
}

if (fstat(fd, &st) < 0 || !S_ISREG(st.st_mode) || st.st_uid != getuid()) {
close(fd);
return;
}

n = read(fd, tmp, VNC_FILE_LEN);
close(fd);

if (n == (ssize_t)VNC_FILE_LEN) {
memcpy(readonly_buf, tmp + VNC_PASSWORD_LEN, VNC_PASSWORD_LEN);
*has_readonly = 1;
explicit_bzero(tmp, VNC_FILE_LEN);
}
}

/*
 * Atomically write the password file.
 *   - Creates a temporary file in the same directory.
 *   - Sets mode 0600 before writing any data.
 *   - Writes the read-write password (always 8 bytes).
 *   - Appends the read-only password bytes if has_ro is set.
 *   - Renames temp → dest.
 *
 * Returns 0 on success, -1 on error (message printed to stderr).
 */
static int write_atomic(const char* dest,
                        const unsigned char* rw_pass,
                        const unsigned char* ro_pass,
                        int has_ro)
{
char* tmppath;
size_t tmpsz;
static const char tmpsuffix[] = ".XXXXXX";
int fd;
ssize_t n;
int ret = 0;

tmpsz = strlen(dest) + sizeof(tmpsuffix);
tmppath = (char*)malloc(tmpsz);
if (tmppath == NULL) {
fprintf(stderr, "vncpasswd: out of memory\n");
return -1;
}
snprintf(tmppath, tmpsz, "%s%s", dest, tmpsuffix);

fd = mkostemp(tmppath, O_CLOEXEC);
if (fd < 0) {
fprintf(stderr, "vncpasswd: mkostemp %s: %s\n", tmppath, strerror(errno));
free(tmppath);
return -1;
}

/* Set permissions before any data is written */
if (fchmod(fd, 0600) != 0) {
fprintf(stderr, "vncpasswd: fchmod: %s\n", strerror(errno));
close(fd);
(void)unlink(tmppath);
free(tmppath);
return -1;
}

/* Write read-write password (8 bytes) */
n = write(fd, rw_pass, VNC_PASSWORD_LEN);
if (n != (ssize_t)VNC_PASSWORD_LEN) {
fprintf(stderr, "vncpasswd: write: %s\n", strerror(errno));
close(fd);
(void)unlink(tmppath);
free(tmppath);
return -1;
}

/* Preserve read-only password if one existed */
if (has_ro) {
n = write(fd, ro_pass, VNC_PASSWORD_LEN);
if (n != (ssize_t)VNC_PASSWORD_LEN) {
fprintf(stderr, "vncpasswd: write (ro): %s\n", strerror(errno));
close(fd);
(void)unlink(tmppath);
free(tmppath);
return -1;
}
}

if (close(fd) != 0) {
fprintf(stderr, "vncpasswd: close: %s\n", strerror(errno));
(void)unlink(tmppath);
free(tmppath);
return -1;
}

/* Atomic rename */
if (rename(tmppath, dest) != 0) {
fprintf(stderr, "vncpasswd: rename to %s: %s\n", dest, strerror(errno));
(void)unlink(tmppath);
ret = -1;
}

free(tmppath);
return ret;
}

int main(int argc, char* argv[])
{
char* dest_path = NULL;
char* cli_override = NULL;
char pass1[MAX_INPUT];
char pass2[MAX_INPUT];
unsigned char obfuscated[VNC_PASSWORD_LEN];
unsigned char readonly_data[VNC_PASSWORD_LEN];
int has_readonly = 0;
int interactive;
int opt;
size_t pass_len;
size_t i;
int ret = 0;
struct passwd* pw;
const char* home;

/* Parse options */
while ((opt = getopt(argc, argv, "f:h")) != -1) {
switch (opt) {
case 'f':
cli_override = optarg;
break;
case 'h':
usage(argv[0]);
return 0;
default:
usage(argv[0]);
return 1;
}
}

if (optind < argc) {
fprintf(stderr, "vncpasswd: unexpected argument: %s\n", argv[optind]);
usage(argv[0]);
return 1;
}

/* Determine home directory for config hierarchy */
pw = getpwuid(getuid());
home = (pw != NULL) ? pw->pw_dir : NULL;

/* Resolve destination path through the full config hierarchy */
dest_path = vnc_find_passwd_file(home, cli_override);
if (dest_path == NULL) {
fprintf(stderr, "vncpasswd: cannot determine password file path\n");
return 1;
}

/* Check whether we're running interactively */
interactive = isatty(STDIN_FILENO);

if (interactive) {
/* Prompt twice and verify they match */
if (read_password_tty("Password: ", pass1, sizeof(pass1)) != 0) {
ret = 1;
goto cleanup;
}
if (read_password_tty("Verify: ", pass2, sizeof(pass2)) != 0) {
explicit_bzero(pass1, sizeof(pass1));
ret = 1;
goto cleanup;
}
if (strcmp(pass1, pass2) != 0) {
fprintf(stderr, "vncpasswd: passwords do not match\n");
explicit_bzero(pass1, sizeof(pass1));
explicit_bzero(pass2, sizeof(pass2));
ret = 1;
goto cleanup;
}
explicit_bzero(pass2, sizeof(pass2));
} else {
/* Non-interactive: read a single line from stdin */
if (read_password_stdin(pass1, sizeof(pass1)) != 0) {
fprintf(stderr, "vncpasswd: failed to read password from stdin\n");
ret = 1;
goto cleanup;
}
}

/* Preserve existing read-only password if present */
read_existing(dest_path, readonly_data, &has_readonly);

/* Build the obfuscated (bit-reversed) password, zero-padded to 8 bytes */
memset(obfuscated, 0, VNC_PASSWORD_LEN);
pass_len = strlen(pass1);
if (pass_len > VNC_PASSWORD_LEN) {
pass_len = VNC_PASSWORD_LEN;
}
for (i = 0; i < pass_len; i++) {
obfuscated[i] = reverse_bits((unsigned char)pass1[i]);
}
explicit_bzero(pass1, sizeof(pass1));

/* Ensure the target directory exists */
if (ensure_dir(dest_path) != 0) {
explicit_bzero(obfuscated, VNC_PASSWORD_LEN);
ret = 1;
goto cleanup;
}

/* Write atomically */
if (write_atomic(dest_path, obfuscated, readonly_data, has_readonly) != 0) {
explicit_bzero(obfuscated, VNC_PASSWORD_LEN);
ret = 1;
goto cleanup;
}

explicit_bzero(obfuscated, VNC_PASSWORD_LEN);

if (interactive) {
printf("Password updated successfully.\n");
}

cleanup:
free(dest_path);
return ret;
}
