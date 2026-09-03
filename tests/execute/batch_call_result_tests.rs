use qubit_batch::BatchCallOutput;
use qubit_batch::BatchCallResult;
use qubit_batch::BatchCallResultBuildError;
use qubit_batch::BatchOutcomeBuilder;
use qubit_batch::BatchTaskError;
use qubit_batch::BatchTaskFailure;

#[test]
fn test_batch_call_result_accessors_and_parts() {
    let outcome = BatchOutcomeBuilder::<&'static str>::builder(2)
        .completed_count(2)
        .succeeded_count(1)
        .panicked_count(1)
        .failures(vec![BatchTaskFailure::new(1, BatchTaskError::panicked("panic"))])
        .build()
        .expect("outcome should be valid");
    let result = BatchCallResult::try_new(outcome.clone(), vec![BatchCallOutput::new(0, 10)])
        .expect("sparse outputs should match the outcome");

    assert_eq!(result.outcome(), &outcome);
    assert_eq!(result.outputs()[0].value(), &10);
    assert_eq!(result.clone().into_outputs()[0].value(), &10);
    assert_eq!(result.clone().into_outcome(), outcome);
    let (outcome_part, outputs_part) = result.into_parts();
    assert_eq!(outcome_part.completed_count(), 2);
    assert_eq!(outputs_part[0].index(), 0);
}

#[test]
fn test_batch_call_result_rejects_output_for_uncompleted_task() {
    let outcome = BatchOutcomeBuilder::<&'static str>::builder(3)
        .completed_count(1)
        .succeeded_count(1)
        .build()
        .expect("outcome should be valid");

    assert_eq!(
        BatchCallResult::<usize, &'static str>::try_new(outcome, vec![BatchCallOutput::new(2, 10)],),
        Err(BatchCallResultBuildError::OutputIndexNotCompleted {
            index: 2,
            completed_count: 1,
        })
    );
}

#[test]
fn test_batch_call_result_rejects_value_at_failed_callable_index() {
    let outcome = BatchOutcomeBuilder::builder(2)
        .completed_count(2)
        .succeeded_count(1)
        .failed_count(1)
        .failures(vec![BatchTaskFailure::new(
            1,
            BatchTaskError::Failed("failed callable"),
        )])
        .build()
        .expect("outcome should be valid");

    assert_eq!(
        BatchCallResult::try_new(outcome, vec![BatchCallOutput::new(0, 10), BatchCallOutput::new(1, 20)],),
        Err(BatchCallResultBuildError::FailureOutputPresent { index: 1 })
    );
}

#[test]
fn test_batch_call_result_rejects_mismatched_success_output_count() {
    let outcome = BatchOutcomeBuilder::<&'static str>::builder(2)
        .completed_count(2)
        .succeeded_count(2)
        .build()
        .expect("outcome should be valid");

    assert_eq!(
        BatchCallResult::try_new(outcome, vec![BatchCallOutput::new(0, 10)],),
        Err(BatchCallResultBuildError::SucceededOutputCountMismatch {
            succeeded_count: 2,
            output_count: 1,
        })
    );
}
