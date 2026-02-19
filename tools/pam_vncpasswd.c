/*
 * pam_vncpasswd - PAM module to authenticate against a user's ~/.vncpasswd
 *
 * The .vncpasswd file stores an 8-byte obfuscated password where each byte
 * is the bit-reversal of the corresponding character in the plaintext
 * password (which is limited to 8 characters).
 */

#include <pwd.h>
#include <security/pam_appl.h>
#include <security/pam_ext.h>
#include <security/pam_modules.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#define VNCPASSWD_FILE "/.vncpasswd"
#define VNC_PASSWORD_LEN 8

/* Reverse the bits in a single byte, matching the VNC obfuscation scheme */
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

/*
 * pam_sm_authenticate - verify that the supplied password matches the
 * obfuscated password stored in the user's ~/.vncpasswd file.
 */
PAM_EXTERN int pam_sm_authenticate(pam_handle_t* pamh, int flags, int argc, const char** argv)
{
	const char* username = NULL;
	const char* password = NULL;
	struct passwd* pw = NULL;
	char* vncpasswd_path = NULL;
	FILE* fp = NULL;
	unsigned char stored[VNC_PASSWORD_LEN];
	unsigned char derived[VNC_PASSWORD_LEN];
	size_t path_len;
	size_t pass_len;
	size_t i;
	int retval;
	int match;

	(void)flags;
	(void)argc;
	(void)argv;

	/* Retrieve the username */
	retval = pam_get_user(pamh, &username, NULL);
	if (retval != PAM_SUCCESS || username == NULL) {
		return PAM_USER_UNKNOWN;
	}

	/* Look up the user's home directory */
	pw = getpwnam(username);
	if (pw == NULL || pw->pw_dir == NULL) {
		return PAM_USER_UNKNOWN;
	}

	/* Retrieve the authentication token (password) */
	retval = pam_get_authtok(pamh, PAM_AUTHTOK, &password, NULL);
	if (retval != PAM_SUCCESS || password == NULL) {
		return PAM_AUTH_ERR;
	}

	/* Build the path to ~/.vncpasswd */
	path_len = strlen(pw->pw_dir) + strlen(VNCPASSWD_FILE) + 1;
	vncpasswd_path = malloc(path_len);
	if (vncpasswd_path == NULL) {
		return PAM_BUF_ERR;
	}
	snprintf(vncpasswd_path, path_len, "%s%s", pw->pw_dir, VNCPASSWD_FILE);

	/* Read the 8-byte obfuscated password from the file */
	fp = fopen(vncpasswd_path, "rb");
	free(vncpasswd_path);
	vncpasswd_path = NULL;
	if (fp == NULL) {
		return PAM_AUTHINFO_UNAVAIL;
	}
	if (fread(stored, 1, VNC_PASSWORD_LEN, fp) != VNC_PASSWORD_LEN) {
		fclose(fp);
		return PAM_AUTHINFO_UNAVAIL;
	}
	fclose(fp);

	/* Derive the obfuscated form of the supplied password:
	 * truncate/pad to 8 bytes and reverse the bits of each byte */
	memset(derived, 0, VNC_PASSWORD_LEN);
	pass_len = strlen(password);
	if (pass_len > VNC_PASSWORD_LEN) {
		pass_len = VNC_PASSWORD_LEN;
	}
	for (i = 0; i < pass_len; i++) {
		derived[i] = reverse_bits((unsigned char)password[i]);
	}

	/* Compare in constant time to avoid timing side-channels:
	 * accumulate XOR differences so no branch depends on the values */
	{
		unsigned char diff = 0;
		for (i = 0; i < VNC_PASSWORD_LEN; i++) {
			diff |= derived[i] ^ stored[i];
		}
		match = (diff == 0);
	}

	/* Zero the derived key before returning; explicit_bzero cannot be
	 * optimized away by the compiler */
	explicit_bzero(derived, VNC_PASSWORD_LEN);

	return match ? PAM_SUCCESS : PAM_AUTH_ERR;
}

/* Required stubs for a complete PAM module */
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
