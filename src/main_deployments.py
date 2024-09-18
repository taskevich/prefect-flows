from prefect import flow

SOURCE_REPO = "https://github.com/taskevich/prefect-flows.git"

if __name__ == "__main__":
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="flows.py:process_csv",
    ).deploy(
        name="my-flow",
        work_pool_name="my-flow",
    )
