#!/bin/bash

ZONE="us-central1-a"
USER="$USER"

parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
        start | stop)
            ACTION="$1"
            shift
            ;;
        --project | -p)
            PROJECT_ID="$2"
            shift 2
            ;;
        --instance | -i)
            INSTANCE_NAME="$2"
            shift 2
            ;;
        --zone | -z)
            ZONE="$2"
            shift 2
            ;;
        --user | -u)
            USER="$2"
            shift 2
            ;;
        --help | -h)
            help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            help
            exit 1
            ;;
        esac
    done

    if [[ -z "$ACTION" || -z "$PROJECT_ID" || -z "$INSTANCE_NAME" ]]; then
        echo "Error: Missing required arguments."
        help
        exit 1
    fi
}

start_instance() {
    config_file="$HOME/.ssh/config"

    [[ ! -f "$config_file" ]] && touch "$config_file"

    gcloud compute instances start "$INSTANCE_NAME" --zone "$ZONE" --project "$PROJECT_ID"

    external_ip=$(
        gcloud compute instances describe "$INSTANCE_NAME" --zone "$ZONE" --project "$PROJECT_ID" | grep natIP | awk '{print $2}'
    )

    if [[ ! -f "$HOME/.ssh/google_compute_engine" ]]; then
        echo "No SSH key found. Please select one from the list below:"
        select key in $(ls $HOME/.ssh/*.pub); do
            echo "Using $key, creating symlink to $HOME/.ssh/google_compute_engine"
            ln -s "$key" $HOME/.ssh/google_compute_engine
            break
        done
    fi

    if grep -q "Host $INSTANCE_NAME:$PROJECT_ID:$ZONE" "$config_file"; then
        sed -i "" "s/HostName .*/HostName $external_ip/" "$config_file"
    else
        [[ $(od -An -tc -N1 "$config_file") != $'\n' ]] && echo >>"$config_file"
        echo "Host $INSTANCE_NAME:$PROJECT_ID:$ZONE" >>"$config_file"
        echo "    HostName $external_ip" >>"$config_file"
        echo "    User $USER" >>"$config_file"
        echo "    IdentityFile $HOME/.ssh/google_compute_engine" >>"$config_file"
    fi
}

stop_instance() {
    config_file="$HOME/.ssh/config"

    if grep -q "Host $INSTANCE_NAME:$PROJECT_ID:$ZONE" "$config_file"; then
        sed -i "" "/Host $INSTANCE_NAME:$PROJECT_ID:$ZONE/,+3d" "$config_file"
        sed -i "" -e :a -e '/^\n*$/{$d;N;ba' -e '}' "$config_file"
    fi

    gcloud compute instances stop "$INSTANCE_NAME" --zone "$ZONE" --project "$PROJECT_ID"
}

help() {
    echo "Usage: $0 start|stop --project <project_id> --instance <instance_name> [--zone <zone>] [--user <user>]"
    echo
    echo "Options:"
    echo "  start|stop          Action to perform (start or stop the instance)"
    echo "  --project, -p       Google Cloud project ID"
    echo "  --instance, -i      Name of the compute instance"
    echo "  --zone, -z          Compute instance zone (default: us-central1-a)"
    echo "  --user, -u          SSH user (default: current system user)"
    echo "  --help, -h          Display this help message"
}

parse_arguments "$@"

if [[ "$ACTION" == "start" ]]; then
    start_instance
elif [[ "$ACTION" == "stop" ]]; then
    stop_instance
else
    help
fi
