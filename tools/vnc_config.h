/*
 * vnc_config.h – VNC password-file path resolution
 *
 * Resolves the location of the VNC password file using the following
 * priority order (later entries override earlier ones):
 *
 *   1. Compiled-in default:       <home>/.vnc/passwd
 *   2. System default config:     /usr/share/vnc/vncpasswd.conf
 *   3. System admin config:       /etc/vnc/vncpasswd.conf
 *   4. User config:               <home>/.config/vnc/vncpasswd.conf
 *   5. Explicit caller override:  CLI -f / PAM module file= argument
 *
 * Config file format:
 *   passwd_file = <path>
 * Lines beginning with '#' and blank lines are ignored.
 * A path beginning with "~/" is expanded using the supplied home directory.
 */

#ifndef VNC_CONFIG_H
#define VNC_CONFIG_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Compiled-in defaults and configuration locations */
#define VNC_DEFAULT_PASSWD_SUFFIX "/.vnc/passwd"
#define VNC_DATADIR_CONF          "/usr/share/vnc/vncpasswd.conf"
#define VNC_SYSCONFDIR_CONF       "/etc/vnc/vncpasswd.conf"
#define VNC_USER_CONF_SUFFIX      "/.config/vnc/vncpasswd.conf"

/*
 * Read the passwd_file value from a single configuration file.
 *
 * Config file format: lines of the form
 *   passwd_file = <path>
 * (whitespace around '=' is ignored; blank lines and '#' comments are skipped).
 * The last matching line in the file wins.
 * A path beginning with "~/" is expanded using home (ignored if home is NULL).
 *
 * Returns a newly malloc'd string, or NULL if the key is not found or on error.
 * The caller must free() the returned string.
 */
static inline char* vnc_read_config_file(const char* config_path, const char* home)
{
	FILE* fp;
	char line[512];
	char* result = NULL;
	static const char key[] = "passwd_file";
	const size_t klen = sizeof(key) - 1;

	if (config_path == NULL) {
		return NULL;
	}

	fp = fopen(config_path, "r");
	if (fp == NULL) {
		return NULL;
	}

	while (fgets(line, (int)sizeof(line), fp) != NULL) {
		char* p = line;
		char* end;
		char* tmp;
		size_t plen;

		/* Strip trailing whitespace / newline */
		end = p + strlen(p);
		while (end > p &&
		       (end[-1] == '\n' || end[-1] == '\r' || end[-1] == ' ' || end[-1] == '\t')) {
			*--end = '\0';
		}

		/* Skip leading whitespace */
		while (*p == ' ' || *p == '\t') {
			p++;
		}

		/* Skip blank lines and comments */
		if (*p == '\0' || *p == '#') {
			continue;
		}

		/* Check for the "passwd_file" key */
		if (strncmp(p, key, klen) != 0) {
			continue;
		}
		p += klen;

		/* Skip optional whitespace before '=' */
		while (*p == ' ' || *p == '\t') {
			p++;
		}
		if (*p != '=') {
			continue;
		}
		p++;

		/* Skip optional whitespace after '=' */
		while (*p == ' ' || *p == '\t') {
			p++;
		}
		if (*p == '\0') {
			continue;
		}

		/* Handle "~/" expansion */
		if (strncmp(p, "~/", 2) == 0 && home != NULL) {
			plen = strlen(home) + 1 + strlen(p + 2) + 1;
			tmp = (char*)malloc(plen);
			if (tmp != NULL) {
				snprintf(tmp, plen, "%s/%s", home, p + 2);
				free(result);
				result = tmp;
			}
		} else {
			tmp = strdup(p);
			if (tmp != NULL) {
				free(result);
				result = tmp;
			}
		}
	}

	fclose(fp);
	return result;
}

/*
 * Determine the VNC password file path using the full configuration hierarchy.
 *
 * home:              User's home directory (may be NULL if unknown).
 * explicit_override: If non-NULL, this value overrides all config-file
 *                    settings.  A leading "~/" is expanded using home.
 *
 * Priority (last wins):
 *   1. Compiled-in default:  <home>/.vnc/passwd
 *   2. /usr/share/vnc/vncpasswd.conf
 *   3. /etc/vnc/vncpasswd.conf
 *   4. <home>/.config/vnc/vncpasswd.conf
 *   5. explicit_override
 *
 * Returns a newly malloc'd string; the caller must free() it.
 * Returns NULL only on memory allocation failure or if home is NULL with
 * no override and no applicable config files.
 */
static inline char* vnc_find_passwd_file(const char* home, const char* explicit_override)
{
	char* path = NULL;
	char* tmp;
	size_t len;

	/* 1. Compiled-in default */
	if (home != NULL) {
		len = strlen(home) + strlen(VNC_DEFAULT_PASSWD_SUFFIX) + 1;
		path = (char*)malloc(len);
		if (path != NULL) {
			snprintf(path, len, "%s%s", home, VNC_DEFAULT_PASSWD_SUFFIX);
		}
	}

	/* 2. System default: /usr/share/vnc/vncpasswd.conf */
	tmp = vnc_read_config_file(VNC_DATADIR_CONF, home);
	if (tmp != NULL) {
		free(path);
		path = tmp;
	}

	/* 3. System admin: /etc/vnc/vncpasswd.conf */
	tmp = vnc_read_config_file(VNC_SYSCONFDIR_CONF, home);
	if (tmp != NULL) {
		free(path);
		path = tmp;
	}

	/* 4. User config: <home>/.config/vnc/vncpasswd.conf */
	if (home != NULL) {
		char* user_conf;
		len = strlen(home) + strlen(VNC_USER_CONF_SUFFIX) + 1;
		user_conf = (char*)malloc(len);
		if (user_conf != NULL) {
			snprintf(user_conf, len, "%s%s", home, VNC_USER_CONF_SUFFIX);
			tmp = vnc_read_config_file(user_conf, home);
			free(user_conf);
			if (tmp != NULL) {
				free(path);
				path = tmp;
			}
		}
	}

	/* 5. Explicit override (highest priority) */
	if (explicit_override != NULL) {
		if (strncmp(explicit_override, "~/", 2) == 0 && home != NULL) {
			len = strlen(home) + 1 + strlen(explicit_override + 2) + 1;
			tmp = (char*)malloc(len);
			if (tmp != NULL) {
				snprintf(tmp, len, "%s/%s", home, explicit_override + 2);
				free(path);
				path = tmp;
			}
		} else {
			tmp = strdup(explicit_override);
			if (tmp != NULL) {
				free(path);
				path = tmp;
			}
		}
	}

	return path;
}

#endif /* VNC_CONFIG_H */
