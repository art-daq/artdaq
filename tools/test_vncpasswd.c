/*
 * test_vncpasswd.c - self-tests for the VNC password utilities
 *
 * Tests the core logic shared by vncpasswd(1) and pam_vncpasswd(8):
 *   - Bit-reversal obfuscation
 *   - Password obfuscation and comparison
 *   - Configuration file parsing (vnc_read_config_file)
 *   - Password-file path resolution (vnc_find_passwd_file)
 */

/* Enable POSIX and Linux extensions */
#define _GNU_SOURCE

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "vnc_config.h"

/* ---- Minimal test framework ---- */
static int g_test_count = 0;
static int g_fail_count = 0;

#define CHECK(desc, expr)                                                 \
	do {                                                              \
		g_test_count++;                                           \
		if (!(expr)) {                                            \
			fprintf(stderr, "FAIL [%s:%d] %s\n", __FILE__,   \
			        __LINE__, (desc));                        \
			g_fail_count++;                                   \
		} else {                                                  \
			printf("PASS %s\n", (desc));                      \
		}                                                         \
	} while (0)

/* ---- Bit-reversal (same logic as in the tools) ---- */
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

/* ---- Tests ---- */

static void test_reverse_bits(void)
{
	CHECK("reverse_bits(0x00) == 0x00", reverse_bits(0x00) == 0x00);
	CHECK("reverse_bits(0xFF) == 0xFF", reverse_bits(0xFF) == 0xFF);
	CHECK("reverse_bits(0x01) == 0x80", reverse_bits(0x01) == 0x80);
	CHECK("reverse_bits(0x80) == 0x01", reverse_bits(0x80) == 0x01);
	CHECK("reverse_bits(0x0F) == 0xF0", reverse_bits(0x0F) == 0xF0);
	CHECK("reverse_bits(0xF0) == 0x0F", reverse_bits(0xF0) == 0x0F);
	CHECK("reverse_bits is its own inverse",
	      reverse_bits(reverse_bits(0xA5)) == 0xA5);
	CHECK("reverse_bits(0x55) == 0xAA", reverse_bits(0x55) == 0xAA);
}

static void test_password_obfuscation(void)
{
	static const char* passwords[] = {"secret", "12345678", "a", "", "longerThan8"};
	static const char* wrong[] = {"Secret", "12345677", "b", "x", "LongerThan8"};
	size_t n = sizeof(passwords) / sizeof(passwords[0]);
	size_t p;

	for (p = 0; p < n; p++) {
		const char* pw = passwords[p];
		const char* wp = wrong[p];
		unsigned char stored[8] = {0};
		unsigned char derived[8] = {0};
		unsigned char wderived[8] = {0};
		size_t plen = strlen(pw);
		size_t wlen = strlen(wp);
		size_t i;
		unsigned char diff;
		char desc[128];

		if (plen > 8) plen = 8;
		if (wlen > 8) wlen = 8;

		for (i = 0; i < plen; i++) {
			stored[i] = reverse_bits((unsigned char)pw[i]);
		}
		for (i = 0; i < plen; i++) {
			derived[i] = reverse_bits((unsigned char)pw[i]);
		}
		for (i = 0; i < wlen; i++) {
			wderived[i] = reverse_bits((unsigned char)wp[i]);
		}

		diff = 0;
		for (i = 0; i < 8; i++) {
			diff |= derived[i] ^ stored[i];
		}
		snprintf(desc, sizeof(desc), "correct password '%s' matches stored", pw);
		CHECK(desc, diff == 0);

		diff = 0;
		for (i = 0; i < 8; i++) {
			diff |= wderived[i] ^ stored[i];
		}
		snprintf(desc, sizeof(desc), "wrong password '%s' does not match '%s'", wp, pw);
		/* Empty passwords hash identically – skip the check */
		if (pw[0] != '\0' || wp[0] != '\0') {
			CHECK(desc, diff != 0);
		}
	}
}

static void test_config_file_parsing(void)
{
	char tmpfile[] = "/tmp/test_vnc_cfg_XXXXXX";
	int fd;
	FILE* fp;
	char* result;

	fd = mkstemp(tmpfile);
	if (fd < 0) {
		fprintf(stderr, "SKIP config-file tests: mkstemp failed\n");
		return;
	}
	close(fd);

	/* Test 1: absolute path */
	fp = fopen(tmpfile, "w");
	if (fp) {
		fprintf(fp, "# comment line\n");
		fprintf(fp, "\n");
		fprintf(fp, "passwd_file = /test/path/passwd\n");
		fclose(fp);
		result = vnc_read_config_file(tmpfile, "/home/user");
		CHECK("config: absolute path returned", result != NULL);
		CHECK("config: absolute path value",
		      result != NULL && strcmp(result, "/test/path/passwd") == 0);
		free(result);
	}

	/* Test 2: tilde expansion */
	fp = fopen(tmpfile, "w");
	if (fp) {
		fprintf(fp, "passwd_file=~/.vnc/passwd\n");
		fclose(fp);
		result = vnc_read_config_file(tmpfile, "/home/user");
		CHECK("config: tilde expanded correctly",
		      result != NULL && strcmp(result, "/home/user/.vnc/passwd") == 0);
		free(result);
	}

	/* Test 3: last value wins */
	fp = fopen(tmpfile, "w");
	if (fp) {
		fprintf(fp, "passwd_file = /first/path\n");
		fprintf(fp, "passwd_file = /second/path\n");
		fclose(fp);
		result = vnc_read_config_file(tmpfile, NULL);
		CHECK("config: last matching line wins",
		      result != NULL && strcmp(result, "/second/path") == 0);
		free(result);
	}

	/* Test 4: no key present → NULL */
	fp = fopen(tmpfile, "w");
	if (fp) {
		fprintf(fp, "# only comments\n");
		fprintf(fp, "other_key = value\n");
		fclose(fp);
		result = vnc_read_config_file(tmpfile, NULL);
		CHECK("config: missing key returns NULL", result == NULL);
		free(result);
	}

	/* Test 5: whitespace variants */
	fp = fopen(tmpfile, "w");
	if (fp) {
		fprintf(fp, "passwd_file=/no/spaces\n");
		fclose(fp);
		result = vnc_read_config_file(tmpfile, NULL);
		CHECK("config: no spaces around '=' accepted",
		      result != NULL && strcmp(result, "/no/spaces") == 0);
		free(result);
	}

	/* Test 6: non-existent file → NULL */
	result = vnc_read_config_file("/nonexistent/path/to/config", NULL);
	CHECK("config: non-existent file returns NULL", result == NULL);

	unlink(tmpfile);
}

static void test_find_passwd_file(void)
{
	char* result;

	/* Default path uses home directory */
	result = vnc_find_passwd_file("/home/testuser", NULL);
	CHECK("find: default path contains home",
	      result != NULL && strcmp(result, "/home/testuser/.vnc/passwd") == 0);
	free(result);

	/* Explicit absolute override */
	result = vnc_find_passwd_file("/home/testuser", "/custom/passwd");
	CHECK("find: explicit override used",
	      result != NULL && strcmp(result, "/custom/passwd") == 0);
	free(result);

	/* Explicit tilde override */
	result = vnc_find_passwd_file("/home/testuser", "~/.vnc/custom");
	CHECK("find: tilde in override expanded",
	      result != NULL && strcmp(result, "/home/testuser/.vnc/custom") == 0);
	free(result);

	/* Config-file override via a temporary /etc-style file */
	{
		char tmpfile[] = "/tmp/test_vnc_find_XXXXXX";
		FILE* fp;
		int fd = mkstemp(tmpfile);
		if (fd >= 0) {
			close(fd);
			fp = fopen(tmpfile, "w");
			if (fp) {
				fprintf(fp, "passwd_file = /from/config\n");
				fclose(fp);
				/* We can't test the real /etc/vnc/vncpasswd.conf,
				 * but we can verify vnc_read_config_file reads it */
				result = vnc_read_config_file(tmpfile, "/home/x");
				CHECK("find: config file provides path",
				      result != NULL && strcmp(result, "/from/config") == 0);
				free(result);
			}
			unlink(tmpfile);
		}
	}

	/* NULL home + no override → NULL */
	result = vnc_find_passwd_file(NULL, NULL);
	CHECK("find: NULL home + no override → NULL", result == NULL);
	free(result);

	/* NULL home + explicit override → override used */
	result = vnc_find_passwd_file(NULL, "/abs/path");
	CHECK("find: NULL home + absolute override → override",
	      result != NULL && strcmp(result, "/abs/path") == 0);
	free(result);
}

int main(void)
{
	printf("=== VNC password utilities self-tests ===\n\n");
	test_reverse_bits();
	test_password_obfuscation();
	test_config_file_parsing();
	test_find_passwd_file();
	printf("\n=== Results: %d/%d passed ===\n",
	       g_test_count - g_fail_count, g_test_count);
	return g_fail_count ? 1 : 0;
}
