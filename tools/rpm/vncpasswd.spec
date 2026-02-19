Name:           vncpasswd
Version:        1.0.0
Release:        1%{?dist}
Summary:        VNC password file management utilities
License:        BSD-3-Clause
URL:            https://github.com/art-daq/artdaq

BuildRequires:  gcc
BuildRequires:  pam-devel
BuildRequires:  cmake >= 3.19

%description
Two tools for managing VNC password files in the TigerVNC-compatible format:

  vncpasswd(1)      – creates or updates ~/.vnc/passwd interactively or
                      non-interactively (pipeline-friendly).

  pam_vncpasswd(8)  – PAM authentication module that validates a supplied
                      password against the bit-reversed obfuscation stored
                      in a user's VNC password file.

Both tools support a layered configuration hierarchy that allows system
administrators to redirect the password file location without user action.

%prep
# When building standalone, extract just the tools/ subdirectory.
# In the artdaq build, cmake is invoked from the top-level source tree.

%build
%cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_VNC_TOOLS=ON
%cmake_build

%install
%cmake_install

# Ensure the system config directory and default config are present
install -d %{buildroot}%{_datadir}/vnc
install -m 0644 tools/vncpasswd.conf \
    %{buildroot}%{_datadir}/vnc/vncpasswd.conf

%check
%ctest --output-on-failure

%files
%license LICENSE
%doc README.md
%{_bindir}/vncpasswd
%{_libdir}/security/pam_vncpasswd.so
%{_mandir}/man1/vncpasswd.1*
%{_mandir}/man8/pam_vncpasswd.8*
%{_datadir}/vnc/vncpasswd.conf

%changelog
* Thu Feb 19 2026 artdaq <artdaq@fnal.gov> - 1.0.0-1
- Initial standalone package of vncpasswd and pam_vncpasswd from artdaq
