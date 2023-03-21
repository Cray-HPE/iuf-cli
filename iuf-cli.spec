#
# MIT License
#
# (C) Copyright 2022-2023 Hewlett Packard Enterprise Development LP
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
%global __python /usr/bin/python3

%define __pipdir %{_builddir}/.pipdir
%define __pyinstaller %{__pipdir}/bin/pyinstaller

Name: %(echo $NAME)
License: MIT License
Summary: Install and Upgrade Framework (IUF) CLI
Version: %(echo $VERSION)
Release: 1
Source: %{name}-%{version}.tar.bz2
Vendor: Hewlett Packard Enterprise Company

%description
This package contains the CLI interface to the Install and Upgrade
Framework API.

%prep
%setup -q

%build
%{__python} -m pip install -t %{__pipdir} -U -r requirements.txt
PYTHONPATH=%{__pipdir} %{__pyinstaller} --onefile iuf

%install
%{__install} -m 755 -D dist/iuf %{buildroot}%{_bindir}/iuf

%clean

%check
sh rpmbuild_check.sh %{buildroot}%{_bindir}/iuf

%files
%license LICENSE
%defattr(-,root,root)
%{_bindir}/iuf