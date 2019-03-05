#!/bin/bash

[ -d ./csv ] && [ -w ./csv ] && echo './csv/ exists and is writable' || echo './csv/ failed check'
