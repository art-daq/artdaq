===================================
artdaq SQA Testing Plan and Records
===================================

Scope
=====

This document defines the testing approach and release-test record keeping for
artdaq to satisfy moderate-level software quality assurance (SQA) expectations.

Test Plan
=========

artdaq testing is organized into three levels:

1. Unit and component tests in the artdaq and related package test suites.
2. Acceptance testing of integrated software changes with artdaq-demo.
3. Experiment-environment acceptance testing (for example, dedicated
   experiment test stands).

Each release candidate should pass level 1 and level 2 testing. Level 3
testing is required when the release includes changes that affect experiment
integrations, deployment, or operations.

Test Cases
==========

The following test case groups define the minimum release-evaluation set:

* Build and packaging checks for supported build configurations.
* Unit/component test execution from the package test suites.
* artdaq-demo run-control and data-flow acceptance tests.
* Configuration/reconfiguration checks for key runtime modes used by
  collaborators.
* Regression checks for defects fixed since the previous release.

When a change affects experiment-specific interfaces, add corresponding
experiment-environment test cases before release approval.

Test Results and Release Procedure
==================================

For each release, maintain a test record containing at least:

* Release identifier (version/tag/commit range).
* Date and responsible tester(s).
* Environment summary (platform/container/toolchain).
* Test case groups executed.
* Pass/fail result for each executed group.
* Notes for failures, waivers, or follow-up actions.

A release is considered tested when all required test groups are completed and
any exceptions are documented with rationale and approval.

