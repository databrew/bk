library(ggplot2)
library(bohemia)
library(tidyverse)
library(grid)
library(gridExtra)
library(ggpubr)
library(extrafont)
loadfonts()
# dimensions: Format 10 x 6 aprox
dims <- c(10, 6)
# dims <- dims * 3
dims <- dims * 0.5
numbers <- 1:100000
numbers <- add_zero(numbers, 6)

ggqrcode <- function(text, color="black", alpha=1) {
  pkg <- "qrcode"
  require(pkg, character.only = TRUE)
  x <- qrcode_gen(text, plotQRcode=F, dataOutput=T)
  x <- as.data.frame(x)
  
  y <- x
  y$id <- rownames(y)
  y <- gather(y, "key", "value", colnames(y)[-ncol(y)])
  y$key = factor(y$key, levels=rev(colnames(x)))
  y$id = factor(y$id, levels=rev(rownames(x)))
  
  ggplot(y, aes_(x=~id, y=~key)) + geom_tile(aes_(fill=~value), alpha=alpha) +
    scale_fill_gradient(low="white", high=color) +
    theme_void() + theme(legend.position='none')
} # https://github.com/GuangchuangYu/yyplot/blob/master/R/ggqrcode.R

# Read seal

library(png)
img <- readPNG('seal.png')

library(magick)
library(cowplot)

 
# Generate pdf

dir.create('pdfs_hh')
for(i in 1:length(numbers)){
  message(i, ' of ', length(numbers))
# for(i in 1:2){
  this_number <- numbers[i]
  marg <- ggplot() + ggplot2::theme_void()# databrew::theme_simple()
  ab <- ggplot() +
    ggplot2::theme_void() +
    # add the seal
    # annotation_raster(img, 
    #                   xmin = -Inf, xmax = Inf, ymin = -Inf, ymax = Inf) +
    # databrew::theme_simple() +
    labs(title = 'Kaya / Household',
         # y = 'www.databrew.cc/qr',
         subtitle = this_number#,
         # caption = 'Kaya'
         )+
    theme(plot.title = element_text(hjust = 0.5, size = 12),
          plot.subtitle = element_text(hjust = 0.5, size = 40),
          plot.caption = element_text(size = 14, hjust = 0.5))
  a <- ggdraw() + 
    draw_image('seal.png', scale = 0.5, valign = 0, vjust = -0.1) +
    draw_plot(ab)
  gl <- ggqrcode(this_number)

  x <- ggarrange(marg, 
           ggarrange(gl, marg, a, marg, nrow = 1,
                     widths = c(7,1, 5,0.5)),
           marg,
           ncol = 1,
           heights = c(1,10,1))
  
  
  ggexport(x,
           filename = paste0('pdfs_hh/', this_number, '.pdf'),
           width = dims[1], height = dims[2])
}
# setwd('pdfs')
# system('pdftk *.pdf cat output all.pdf')
# setwd('..')

# Combine to 12 per page
setwd('pdfs_hh/')
dir.create('to_print')
n <- length(numbers)
ends <- (1:n)[1:n %% 12 == 0]
starts <- ends - 11

for(i in 1:length(starts)){
  this_start <- starts[i]
  this_end <- ends[i]
  these_numbers <- numbers[this_start:this_end]
  these_numbers <- add_zero(these_numbers, 3)
  these_files <- paste0(these_numbers, '.pdf')
  file_string <- paste0(these_files, collapse = ' ')
  out_file <- paste0('to_print/', add_zero(numbers[this_start], 3), '-',
                     add_zero(numbers[this_end], 3), '.pdf')
  command_string <- paste0('pdfjam ', file_string,
                           ' --nup 3x4 --landscape --outfile ',
                           out_file)
  system(command_string)
}
# Put all in one doc
setwd('to_print')


system('pdftk *.pdf cat output all.pdf')
# Put all 
setwd('..')
setwd('..')
# 
# system('pdfjam 001.pdf 002.pdf 003.pdf 004.pdf 005.pdf 006.pdf 007.pdf 008.pdf 009.pdf 010.pdf 011.pdf 012.pdf --nup 3x4 --landscape --outfile 001-004.pdf')
# setwd('..')
