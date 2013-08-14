#!/usr/bin/env Rscript
# clean-up session parameters
rm(list=ls())

args <- commandArgs(TRUE)
if (length(args)!=1) {
    print(sprintf("Usage: script <file.csv>"))
    quit("no",0,F)
}

# load data
my.path <- paste0(getwd(), "/")
file.name <- args[1]

# load data and plot them
df <- read.csv(paste0(my.path, file.name), header=TRUE)
pdf("monitor.pdf")
par(mfrow=c(2,2))
plot(df$time, df$cpu, type="l", xlab="Time", ylab="CPU")
plot(df$time, df$swap.percent, type="l", xlab="Time", ylab="Swap (%)")
plot(df$time, df$vmem.percent, type="l", xlab="Time", ylab="VMEM (%)")
plot(df$time, df$vmem.free, type="l", xlab="Time", ylab="VMEM (free)")
dev.off()

