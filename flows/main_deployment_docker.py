from flows import process_csv

if __name__ == "__main__":
    process_csv.deploy(
        name="my-flow-docker-deployment",
        work_pool_name="my-flow-worker",
        image="my-flow-docker-image:dev",
        push=False
    )
