#!sh
if [ -n "$1" ]; then
    # $1 is not empty

    if [ -n "$2" ] && [ "$2" == "y" ]; then
        # $2 is "y", delete the found results
        find ~/.gradle/caches/modules-2 -name "$1" -exec rm -rf {} \;

        find ~/.m2/repository -name "$1" -exec rm -rf {} \;
    else
        # $2 is not "y", just print the found results
        echo "Gradle find result: "
	find ~/.gradle/caches/modules-2 -name "$1"
        echo "Maven find result: "
	find ~/.m2/repository -name "$1"
    fi
else
    # $1 is empty
    echo "\$1 is empty"
fi
