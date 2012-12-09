#!/bin/bash

xmlrpc http://localhost:5440/RPC2 ds50.init "daq: {max_fragment_size_words: 524288 fragment_receiver: {mpi_buffer_count: 2 generator:V172xSimulator freqs_file: \"/home/biery/nov2012/ds50daq/ds50daq/DAQ/ds50_hist.dat\" fragments_per_event: 1 nChannels: 3000 generator_ds50: {fragment_id: 0} first_event_builder_rank: 2 event_builder_count: 2}}" &
xmlrpc http://localhost:5441/RPC2 ds50.init "daq: {max_fragment_size_words: 524288 fragment_receiver: {mpi_buffer_count: 2 generator:V172xSimulator freqs_file: \"/home/biery/nov2012/ds50daq/ds50daq/DAQ/ds50_hist.dat\" fragments_per_event: 1 nChannels: 3000 generator_ds50: {fragment_id: 1} first_event_builder_rank: 2 event_builder_count: 2}}" &

xmlrpc http://localhost:5450/RPC2 ds50.init "daq: {max_fragment_size_words: 524288 event_builder: {mpi_buffer_count: 8 first_fragment_receiver_rank: 0 fragment_receiver_count: 2 useArt: false print_event_store_stats: true}}" &
xmlrpc http://localhost:5451/RPC2 ds50.init "daq: {max_fragment_size_words: 524288 event_builder: {mpi_buffer_count: 8 first_fragment_receiver_rank: 0 fragment_receiver_count: 2 useArt: false print_event_store_stats: true}}" &

wait
