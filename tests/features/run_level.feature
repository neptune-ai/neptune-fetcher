Feature: Support for read-only runs

  Scenario: Initialize read-only run
    Given we have a read-only project
     When we initialize the read-only run
     Then no exception is thrown

  Scenario: Initialize read-only run using custom id
    Given we have a read-only project
     When we initialize the read-only run with custom id
     Then no exception is thrown

  Scenario: Initialize read-only run using experiment name
    Given we have a read-only project
     When we initialize the read-only run with experiment name
     Then no exception is thrown

  Scenario: Fetching field names
    Given we have a read-only run
     When we fetch the field names
     Then all field names are present

  Scenario: Fetching int field
    Given we have a read-only run
      And we have an integer field
     When we fetch the field value
     Then the value is 5

  Scenario: Fetching float field
    Given we have a read-only run
      And we have a float field
     When we fetch the field value
     Then the value is 3.14

  Scenario: Fetching string field
    Given we have a read-only run
      And we have a string field
     When we fetch the field value
     Then the value is `Neptune Rulez!`

  Scenario: Fetching bool field
    Given we have a read-only run
      And we have a bool field
     When we fetch the field value
     Then the value is False

  Scenario: Fetching datetime field
    Given we have a read-only run
      And we have a datetime field
     When we fetch the field value
     Then we have a value

  Scenario: Fetching string set field
    Given we have a read-only run
      And we have a string set field
     When we fetch the field value
     Then we have a value

  Scenario: Fetching float series values
    Given we have a read-only run
      And we have a float series
     When we fetch the series values
     Then the values are [1, 2, 4]

  Scenario: Fetching float series last value
    Given we have a read-only run
      And we have a float series
     When we fetch the series last value
     Then the value is 4
