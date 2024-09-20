from kfp import dsl
from kfp.client import Client
from kfp.compiler import Compiler

@dsl.component
def inner_comp() -> str:
    return "foobar"


@dsl.component
def outer_comp(input: str):
    print("input: ", input)


@dsl.pipeline
def inner_pipeline() -> str:
    inner_comp_task = inner_comp()
    inner_comp_task.set_caching_options(False)
    return inner_comp_task.output

@dsl.pipeline
def outer_pipeline():
    inner_pipeline_task = inner_pipeline()
    outer_comp_task = outer_comp(input=inner_pipeline_task.output)
    outer_comp_task.set_caching_options(False)


if __name__ == "__main__":
    # Compiler().compile(outer_pipeline, "ignore/subdag_ir.yaml")
    client = Client()

    run = client.create_run_from_pipeline_func(
        pipeline_func=outer_pipeline,
        enable_caching=False,
    )
