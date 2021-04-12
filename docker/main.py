import argparse


def main():

    parser = argparse.ArgumentParser(description="Process files with pypel")
    parser.add_argument(
        "paths",
        metavar="path",
        type=str,
        nargs="+",
        help="a path (or glob) of file(s) to process",
    )

    args = parser.parse_args()

    # Just print what we should process for now
    print(args.paths)


if __name__ == "__main__":
    main()
