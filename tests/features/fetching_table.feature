Feature: Fetching runs dataframe

  Scenario: Fetch all runs
    Given we have a read-only project
     When we fetch runs dataframe
     Then we should get 2 runs

  Scenario: Filter by `with_ids`
    Given we have a read-only project
      And we filter by `with_ids`
     When we fetch runs dataframe
     Then we should get 1 run

  Scenario: Limit the number of runs
    Given we have a read-only project
      And we limit the number of runs to 1
     When we fetch runs dataframe
     Then we should get 1 run

  Scenario: Limit the number of columns
    Given we have a read-only project
      And we select only 1 column
     When we fetch runs dataframe
     Then we should get 2 runs with 1 column

  Scenario: Sort runs
    Given we have a read-only project
      And we sort by `fields/float` by descending order
     When we fetch runs dataframe
     Then we should get 2 runs sorted by `fields/float` in descending order

  Scenario: Column names regex
    Given we have a read-only project
      And we select columns by regex
     When we fetch runs dataframe
     Then we should get 2 runs
      And we should have selected columns included

  Scenario: Run names regex
    Given we have a read-only project
      And we filter by run names regex
     When we fetch runs dataframe
     Then we should get second run

  Scenario: Custom id regex
    Given we have a read-only project
      And we filter by custom id regex
     When we fetch runs dataframe
     Then we should get second run
