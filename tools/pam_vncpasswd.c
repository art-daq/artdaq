/*
 * pam_vncpasswd - PAM module to authenticate against a user's VNC password file
 *
 * Compatible with the TigerVNC ~/.vnc/passwd file format: the file stores
 * 8 bytes where each byte is the bit-reversal of the corresponding character
 * of the plaintext password (which is limited to 8 characters).  Bytes 8–15,
 * if present, hold an optional read-only password and are ignored here.
 *
 * Module arguments:
 *   file=<path>  Override the password-file path determined by the
 *                configuration hierarchy.  A leading "~/" is expanded to
 *                the authenticating user's home directory.
 *   nullok       Permit empty (zero-length) passwords.  Without this option
 *                an empty password always fails authentication.
 *   debug        Log additional diagnostics to syslog.
 *
 * Configuration hierarchy (later entries override earlier ones):
 *   1. Compiled-in default:       <home>/.vnc/passwd
 *   2. System default config:     /usr/share/vnc/vncpasswd.conf
 *   3. System admin config:       /etc/vnc/vncpasswd.conf
 *   4. User config:               <home>/.config/vnc/vncpasswd.conf
 *   5. Module argument file=      (highest priority)
 */

/* Enable POSIX and Linux extensions (strdup, O_NOFOLLOW, explicit_bzero, etc.) */
#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <pwd.h>
#include <security/pam_appl.h>
#include <security/pam_ext.h>
#include <security/pam_modules.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "vnc_config.h"

#define VNC_PASSWORD_LEN 8

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

/* Parse module arguments into caller-supplied fields */
static void parse_args(int argc, const char** argv, const char** file_override, int* nullok, int* debug)
{
	int i;
	*file_override = NULL;
	*nullok = 0;
	*debug = 0;
	for (i = 0; i < argc; i++) {
		if (strncmp(argv[i], "file=", 5) == 0) {
			*file_override = argv[i] + 5;
		} else if (strcmp(argv[i], "nullok") == 0) {
			*nullok = 1;
		} else if (strcmp(argv[i], "debug") == 0) {
			*debug = 1;
		}
	}
}

/*
 * Open the VNC password file with TOCTOU protection and read the first
 * VNC_PASSWORD_LEN bytes into buf.
 *
 * Protections applied:
 *   • O_NOFOLLOW: fail if the final path component is a symbolic link.
 *   • fstat: verify the open file is a regular file owned by expected_uid
 *     and not readable by group or others (warn if it is).
 *
 * Returns  0 on success,
 *         -2 if the file is not found or is a symlink (AUTHINFO_UNAVAIL),
 *         -1 on any other error.
 */
static int read_vncpasswd(pam_handle_t* pamh, const char* path, uid_t expected_uid, unsigned char* buf)
{
	int fd;
	struct stat st;
	ssize_t n;

	fd = open(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
	if (fd < 0) {
		if (errno == ENOENT || errno == ELOOP) {
			return -2;
		}
		pam_syslog(pamh, LOG_ERR, "pam_vncpasswd: cannot open %s: %m", path);
		return -1;
	}

	if (fstat(fd, &st) < 0) {
		pam_syslog(pamh, LOG_ERR, "pam_vncpasswd: fstat %s: %m", path);
		close(fd);
		return -1;
	}

	/* Must be a regular file */
	if (!S_ISREG(st.st_mode)) {
		pam_syslog(pamh, LOG_ERR, "pam_vncpasswd: %s is not a regular file", path);
		close(fd);
		return -1;
	}

	/* Must be owned by the authenticating user */
	if (st.st_uid != expected_uid) {
		pam_syslog(pamh, LOG_ERR, "pam_vncpasswd: %s is not owned by uid %u", path, (unsigned)expected_uid);
		close(fd);
		return -1;
	}

	/* Warn if group or other can read the password file */
	if (st.st_mode & (S_IRGRP | S_IROTH)) {
		pam_syslog(pamh, LOG_WARNING, "pam_vncpasswd: %s is group/world readable", path);
	}

	/* File must contain at least VNC_PASSWORD_LEN bytes */
	if (st.st_size < (off_t)VNC_PASSWORD_LEN) {
		pam_syslog(pamh, LOG_ERR, "pam_vncpasswd: %s is too short", path);
		close(fd);
		return -2;
	}

	n = read(fd, buf, VNC_PASSWORD_LEN);
	close(fd);

	if (n != (ssize_t)VNC_PASSWORD_LEN) {
		pam_syslog(pamh, LOG_ERR, "pam_vncpasswd: short read from %s", path);
		return -1;
	}

	return 0;
}

/*
 * pam_sm_authenticate - verify the supplied password against the VNC
 * password file.
 */
PAM_EXTERN int pam_sm_authenticate(pam_handle_t* pamh, int flags, int argc, const char** argv)
{
	const char* username = NULL;
	const char* password = NULL;
	const char* file_override = NULL;
	struct passwd* pw = NULL;
	char* vncpasswd_path = NULL;
	unsigned char stored[VNC_PASSWORD_LEN];
	unsigned char derived[VNC_PASSWORD_LEN];
	size_t pass_len;
	size_t i;
	int retval;
	int nullok;
	int debug_flag;
	int rc;
	int match;

	parse_args(argc, argv, &file_override, &nullok, &debug_flag);

	/* Retrieve the username */
	retval = pam_get_user(pamh, &username, NULL);
	if (retval != PAM_SUCCESS || username == NULL) {
		return PAM_USER_UNKNOWN;
	}

	/* Look up the user's home directory and uid */
	pw = getpwnam(username);
	if (pw == NULL || pw->pw_dir == NULL) {
		return PAM_USER_UNKNOWN;
	}

	/* Retrieve the authentication token (password) */
	retval = pam_get_authtok(pamh, PAM_AUTHTOK, &password, NULL);
	if (retval != PAM_SUCCESS || password == NULL) {
		return PAM_AUTH_ERR;
	}

	/* Reject empty passwords unless "nullok" was specified.
	 * PAM_DISALLOW_NULL_AUTHTOK allows the application to override "nullok". */
	if (password[0] == '\0') {
		if ((flags & PAM_DISALLOW_NULL_AUTHTOK) || !nullok) {
			if (debug_flag) {
				pam_syslog(pamh, LOG_DEBUG, "pam_vncpasswd: empty password rejected for %s", username);
			}
			return PAM_AUTH_ERR;
		}
	}

	/* Build the path to the VNC password file using the config hierarchy */
	vncpasswd_path = vnc_find_passwd_file(pw->pw_dir, file_override);
	if (vncpasswd_path == NULL) {
		return PAM_BUF_ERR;
	}

	/* Read and validate the stored password with TOCTOU protection */
	rc = read_vncpasswd(pamh, vncpasswd_path, pw->pw_uid, stored);
	free(vncpasswd_path);

	if (rc == -2) {
		return PAM_AUTHINFO_UNAVAIL;
	}
	if (rc != 0) {
		return PAM_AUTH_ERR;
	}

	/* Derive the obfuscated form of the supplied password:
	 * truncate to 8 characters, zero-pad, then reverse the bits */
	memset(derived, 0, VNC_PASSWORD_LEN);
	pass_len = strlen(password);
	if (pass_len > VNC_PASSWORD_LEN) {
		pass_len = VNC_PASSWORD_LEN;
	}
	for (i = 0; i < pass_len; i++) {
		derived[i] = reverse_bits((unsigned char)password[i]);
	}

	/* Constant-time comparison: accumulate XOR differences so no
	 * data-dependent branch can leak the comparison result */
	{
		unsigned char diff = 0;
		for (i = 0; i < VNC_PASSWORD_LEN; i++) {
			diff |= derived[i] ^ stored[i];
		}
		match = (diff == 0);
	}

	/* Zero sensitive data before returning */
	explicit_bzero(derived, VNC_PASSWORD_LEN);
	explicit_bzero(stored, VNC_PASSWORD_LEN);

	if (debug_flag) {
		pam_syslog(pamh, LOG_DEBUG, "pam_vncpasswd: authentication %s for %s",
		           match ? "succeeded" : "failed", username);
	}

	return match ? PAM_SUCCESS : PAM_AUTH_ERR;
}

PAM_EXTERN int pam_sm_setcred(pam_handle_t* pamh, int flags, int argc, const char** argv)
{
	(void)pamh;
	(void)flags;
	(void)argc;
	(void)argv;
	return PAM_SUCCESS;
}

PAM_EXTERN int pam_sm_acct_mgmt(pam_handle_t* pamh, int flags, int argc, const char** argv)
{
	(void)pamh;
	(void)flags;
	(void)argc;
	(void)argv;
	return PAM_SUCCESS;
}

PAM_EXTERN int pam_sm_open_session(pam_handle_t* pamh, int flags, int argc, const char** argv)
{
	(void)pamh;
	(void)flags;
	(void)argc;
	(void)argv;
	return PAM_SUCCESS;
}

PAM_EXTERN int pam_sm_close_session(pam_handle_t* pamh, int flags, int argc, const char** argv)
{
	(void)pamh;
	(void)flags;
	(void)argc;
	(void)argv;
	return PAM_SUCCESS;
}

/*
 * pam_sm_chauthtok - password-change entry point.
 * This module does not support in-band password changes; use the
 * vncpasswd(1) utility instead.
 */
PAM_EXTERN int pam_sm_chauthtok(pam_handle_t* pamh, int flags, int argc, const char** argv)
{
	(void)pamh;
	(void)flags;
	(void)argc;
	(void)argv;
	return PAM_AUTHTOK_ERR;
}
