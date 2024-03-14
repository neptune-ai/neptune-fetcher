Feature: Read-only project

  Scenario: Initialize read-only project
     When we initialize the read-only project
     Then no exception is thrown

  Scenario: Listing runs
    Given we have a read-only project
     When we list runs
     Then runs list is not empty
      And runs list contains the run details we created
