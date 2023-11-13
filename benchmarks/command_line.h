// modifications:
// Copyright (c) 2023 ETH-Zurich.
//                    All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// main author: Robert Gerstenberger

// original code from GAP Benchmark Suite
// Class:  CLBase
// Author: Scott Beamer
//
// Handles command line argument parsing
// - Through inheritance, can add more options to object
// - For example, most kernels will use CLApp
//
// Copyright (c) 2015, The Regents of the University of California (Regents)
// See LICENSE.txt for license details
//
// original license:
// Copyright (c) 2015, The Regents of the University of California (Regents).
// All Rights Reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 3. Neither the name of the Regents nor the
//    names of its contributors may be used to endorse or promote products
//    derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL REGENTS BE LIABLE FOR ANY
// DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#ifndef __LPG_GRAPH500_GDI_COMMAND_LINE_H
#define __LPG_GRAPH500_GDI_COMMAND_LINE_H

#include <getopt.h>

#include <cinttypes>
#include <iostream>
#include <string>
#include <vector>

class CLBase {
 protected:
  int argc_;
  char** argv_;
  std::string name_;
  std::string get_args_ = "b:de:f:hi:l:m:n:or:s:t:v:w:";
  std::vector<std::string> help_strings_;

  uint32_t blocksize_ = 512;
  double dampingfactor_ = 0.85;
  bool directed_ = false;
  double duration_ = 5;
  uint32_t edgefactor_ = 16;
  std::string filename_ = "";
  uint32_t iterations_ = 5;
  uint32_t layers_ = 5;
  uint64_t memorysize_ = 4096;
  uint64_t nglobalverts_ = 0;
  uint32_t rcount_ = 200;
  uint32_t SCALE_ = 3;
  bool     startAtOne_ = false;
  uint32_t vector_ = 500;

  void AddHelpLine(char opt, std::string opt_arg, std::string text,
                   std::string def = "") {
    const int kBufLen = 100;
    char buf[kBufLen];
    if (opt_arg != "")
      opt_arg = "<" + opt_arg + ">";
    if (def != "")
      def = "[" + def + "]";
    snprintf(buf, kBufLen, " -%c %-10s: %-54s%10s", opt, opt_arg.c_str(),
            text.c_str(), def.c_str());
    help_strings_.push_back(buf);
  }

  public:
  CLBase(int argc, char** argv, std::string name = "") :
         argc_(argc), argv_(argv), name_(name) {
    AddHelpLine('b', "bsize", "block size", "512");
    AddHelpLine('d', "", "use directed edges", "false");
    AddHelpLine('e', "efactor", "edge factor", "16");
    AddHelpLine('f', "file", "load graph from file", "");
    AddHelpLine('i', "iter", "iterations for CDLP/PageRank/WCC", "5");
    AddHelpLine('l', "layers", "layers for GNN", "5");
    AddHelpLine('m', "msize", "memory size per process", "4096");
    AddHelpLine('n', "verts", "number of vertices", "0");
    AddHelpLine('o', "", "vertex UIDs start at one", "false");
    AddHelpLine('r', "rcount", "number of queries", "200");
    AddHelpLine('s', "scale", "log_2(# vertices)", "3");
    AddHelpLine('t', "time", "duration to run Linkbench queries", "5");
    AddHelpLine('v', "vector", "size of feature vector for GNN", "500");
    AddHelpLine('w', "damp", "damping factor for PageRank", "0.85");
    AddHelpLine('h', "", "print this help message");
  }

  bool ParseArgs() {
    signed char c_opt;
    extern char *optarg;          // from and for getopt
    while ((c_opt = getopt(argc_, argv_, get_args_.c_str())) != -1) {
      HandleArg(c_opt, optarg);
    }
    return true;
  }

  void virtual HandleArg(signed char opt, char* opt_arg) {
    switch (opt) {
      case 'b': blocksize_ = strtoul(opt_arg, NULL, 10);     break;
      case 'd': directed_ = true;                            break;
      case 'e': edgefactor_ = strtoul(opt_arg, NULL, 10);    break;
      case 'f': filename_ = std::string(opt_arg);            break;
      case 'h': PrintUsage();                                break;
      case 'i': iterations_ = strtoul(opt_arg, NULL, 10);    break;
      case 'l': layers_ = strtoul(opt_arg, NULL, 10);        break;
      case 'm': memorysize_ = strtoull(opt_arg, NULL, 10);   break;
      case 'n': nglobalverts_ = strtoull(opt_arg, NULL, 10); break;
      case 'o': startAtOne_ = true;                          break;
      case 'r': rcount_ = strtoul(opt_arg, NULL, 10);        break;
      case 's': SCALE_ = strtoul(opt_arg, NULL, 10);         break;
      case 't': duration_ = strtod(opt_arg, NULL);           break;
      case 'v': vector_ = strtoul(opt_arg, NULL, 10);        break;
      case 'w': dampingfactor_ = strtod(opt_arg, NULL);      break;
    }
  }

  void PrintUsage() {
    std::cout << name_ << std::endl;
    for (std::string h : help_strings_)
      std::cout << h << std::endl;
    std::exit(0);
  }

  uint32_t blocksize() const { return blocksize_; }
  double dampingfactor() const { return dampingfactor_; }
  bool directed() const { return directed_; }
  double duration() const { return duration_; }
  uint32_t edgefactor() const { return edgefactor_; }
  std::string filename() const { return filename_; }
  uint32_t iterations() const { return iterations_; }
  uint32_t nlayers() const { return layers_; }
  uint64_t memorysize() const { return memorysize_; }
  uint64_t nglobalverts() const { return nglobalverts_; }
  bool startAtOne() const { return startAtOne_; }
  uint32_t rcount() const { return rcount_; }
  uint32_t scale() const { return SCALE_; }
  uint32_t feature_vector_size() const { return vector_; }
};

#endif // __LPG_GRAPH500_GDI_COMMAND_LINE_H
