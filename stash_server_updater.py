"""
stash_server_updater.py instructions

Purpose:
This tool taps into stashes graphql apis to allow for more complex file processing than is available in the existing gui or 3rd party plugin
options (to my knowledge). I just developed this tool for my own specific use case. I don't plan on giving a lot of support on issues, but I
wanted to share it for anyone familiar with python so they could use it as a starting point for similar tasks. Check out the APIs in
http://localhost:9999/playground . The syntax can be a little confusing so I usually check with the playground before adding a new query as a class
variable.

Supported Features:
--to_delete: allows you to delete files on disk without removing the stash entry by checking for to_delete tag. I use a plugin called QuickEdit
    https://github.com/S3L3CT3DLoves/stashPlugins/blob/main/plugins/QuickEdit/README.md to quickly add to_delete tags when I'm going through media to organize
    it. Periodically I come here and run "python3 stash_server_updater.py --to_delete" to remove all the files I don't want. I do this instead of using stash's
    built in delete option because I want to KEEP the stash entry and its metadata (including phash) but delete the file to free up disk space. I download porn
    in bulk a lot or may not remember I already have a specific scene. This method also checks for re-adds (deleted the file then re-added it again at a later date),
    so I don't have to go through them twice. It also helps if I'm browsing for new content and can check by filename to see if I already moved the scene to the 
    "deleted" pile.

--to_img: I also use this script to bulk add image clips from short videos. Right now I just use this for OF rips. The workflow is the same as to_delete. I use
    QuickEdit to add the to_img tag to scenes I want to make into an image clip (https://docs.stashapp.cc/in-app-manual/images/?h=image+clip#image-clipsgifs). I will
    the periodically run "python3 stash_server_updater.py --to_img" to convert those short clips to image clips so I can browse them with my other images from the same
    "creator". This method also updates the Performer from the video version and adds it to the image clip. Currently I have all OF creators in the same directory 
    self.creator_dir . If your library isn't set up like this you will have to modify the code.

--filename_parser: WIP. Idea is to add more complex parsing logic for custom use cases.

--issue_86: custom parser for stash_vr issue 86 https://github.com/o-fl0w/stash-vr/issues/86 to help people find the bad file due to date formatting that crashes
    Heresphere. Will dump the file name(s) so you can update the date manually as there is no good way to hunt for this with the GUI in big libraries. Follow setup
    steps below then run the following command.
    python3 ./stash_server_updater.py --issue_86

Setup:
1. pip install "gql==3.4.1" . Must use this older version.
2. Create 4x helper tags via the stash gui: "to_delete", "deleted", "to_img", "processing".
    If you already have these exact tags in your db, you will need to modify the code so it doesn't mess up your own
    use case.
3. Get the tag ids by running "python3 stash_server_updater.py --get_helper_tag_ids"
4. Add those numerical values in self.tag_id
5. Add your ApiKey in self.headers

--to_img specific setup:
1. Update self.creator_dir
2. Create cleanup directory (just a helper directory) and update self.cleanup_dir 

Note: I haven't tested if you aren't using the default port of 9999 so that may require additional tweaks.
"""

import json
import pdb
import os
import shutil
import argparse
import csv
import copy
import re

from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport


class StashUtils:

    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--to_img", action="store_true")
        parser.add_argument("--to_delete", action="store_true")
        parser.add_argument("--filename_parser", action="store_true")
        parser.add_argument("--get_helper_tag_ids", action="store_true")
        parser.add_argument("--test", action="store_true")
        parser.add_argument("--issue_86", action="store_true")
        self.args = parser.parse_args()
        self.url = "http://localhost:9999/graphql"
        self.headers = {
            "ApiKey" : "",
            "Content-Type" : "application/json"
        }
        self.tag_id = {
          "TO_DELETE": 1817,
          "DELETED": 1818,
          "TO_IMG": 1819,
          "PROCESSING": 1820
        
        }
        self.creator_dir = "/mnt/wd_red/2d/img/private/creator"
        self.cleanup_dir = "/mnt/wd_red/cleanup"
        self.filename_parser_target_dir = "/mnt/wd_red/2d/vid/public/bareback_studios"
        self.tag_name = {v: k for k, v in self.tag_id.items()}
        self.transport = AIOHTTPTransport(url=self.url, headers=self.headers)
        self.client = Client(transport=self.transport, fetch_schema_from_transport=True)
        
        # Queries =============================================
        self.findTags_by_name = gql("""
          query findTags($tag_name: String!) {
            findTags(tag_filter: {name: {value: $tag_name, modifier: EQUALS}}) {
              tags {
                id
                name
              }
            }
          }                          
        """)
        self.findScenes_by_tag_id = gql("""
          query findScenes($tag_id: [ID!]) {
            findScenes(scene_filter: {tags: {value: $tag_id, modifier: INCLUDES}}) {
              scenes {
                id
                files {
                  path
                }
                tags {
                  name
                  id
                }
                performers {
                    id
                    name
                }
              }
            }
          }
        """)
        self.findPerformers_by_name = gql("""
          query findPerformers($perf_name: String!) {
            findPerformers(performer_filter: {name: {value: $perf_name, modifier: EQUALS}}) {
                performers {
                id
                name
                }
                count
            }
            }                        
        """)
        self.findStudios_by_name = gql("""
            query findStudios($studio_name: String!) {
                findStudios(studio_filter: {name: {value: $studio_name, modifier: EQUALS}}) {
                    studios {
                        id
                        name
                    }
                }
            }
        """)
        self.findScene_by_id = gql("""
            query findScene($id: ID!) {
                findScene(id: $id){
                    id
                    title
                    tags {
                    id
                    name
                    }
                }
                }
         """)
        self.findScenes_by_tag_and_tagex = gql("""
            query findScenes($tag_id: [ID!], $tag_id_ex: [ID!]) {
                findScenes(
                    scene_filter: {tags: {value: $tag_id, modifier: INCLUDES}, 
                    AND: {tags: {value: $tag_id_ex, modifier: EXCLUDES}}}
                ) {
                    scenes {
                    id
                    files {
                        path
                    }
                    tags {
                        name
                        id
                    }
                    performers {
                        id
                        name
                    }
                    }
                }
                }
        """)
        self.findScenes_by_path = gql("""
            query findScenes($path: String!) {
            findScenes(scene_filter: {path: {value: $path, modifier: EQUALS}}) {
                scenes {
                id
                files {
                    path
                }
                }
            }
            }
        """)
        self.findImages_by_path = gql("""
            query findImages($path: String!) {
                findImages(image_filter: {path: {value: $path, modifier: EQUALS}}) {
                    images {
                    id
                    visual_files {
                        ... on ImageFile {
                        id
                        path
                        }
                        ... on VideoFile {
                        id
                        path
                        }
                    }
                    }
                }
                }
        """)
        self.findScenes_by_date_valid = gql("""
            query findScenes($date: String!, $tag_id_ex: [ID!]) {
            findScenes(
                scene_filter: {
                date: { value: $date, modifier: NOT_NULL }
                AND: { tags: { value: $tag_id_ex, modifier: EXCLUDES } }
                }
            ) {
                scenes {
                id
                files {
                    path
                }
                tags {
                    id
                    name
                }
                date
                }
            }
            }
        """)
        self.my_queries = {
            "findScenes_by_tag_id" : self.findScenes_by_tag_id,
            "findScenes_by_path" : self.findScenes_by_path,
            "findTags_by_name": self.findTags_by_name,
            "findStudios_by_name": self.findStudios_by_name,
            "findPerformers_by_name": self.findPerformers_by_name,
            "findScenes_by_tag_and_tagex": self.findScenes_by_tag_and_tagex,
            "findImages_by_path": self.findImages_by_path,
            "findScenes_by_date_valid": self.findScenes_by_date_valid
        }
        
        # Mutations =============================================
        self.sceneUpdate_by_tag_id = gql("""
          mutation sceneUpdate($scene_id: ID!, $tag_id_str: [ID!]) {
            sceneUpdate(input: {id: $scene_id, tag_ids: $tag_id_str}) {
              id
              files {
                path
              }
              tags {
                id
                name
              }
            }
          }
          """)
        self.sceneUpdate_rating = gql("""
          mutation sceneUpdate($scene_id: ID!, $tag_id_str: [ID!]) {
            sceneUpdate(input: {id: $scene_id, tag_ids: $tag_id_str}) {
              id
              files {
                path
              }
              tags {
                id
                name
              }
            }
          }
          """)
        self.sceneUpdate_multiple= gql("""
          mutation sceneUpdate($scene_id: ID!, $studio_id: ID, $title: String, $performer_ids: [ID!]) {
            sceneUpdate(
                input: {id: $scene_id, studio_id: $studio_id, title: $title, performer_ids: $performer_ids}
            ) {
                id
                files {
                path
                }
                tags {
                id
                name
                }
                studio {
                id
                name
                }
                performers {
                id
                name
                }
            }
            }
          """)
        self.imageUpdate_with_performer = gql("""
            mutation imagesUpdate($image_id: ID!, $performer_id: [ID!]) {
                imagesUpdate(input: [{id: $image_id, performer_ids: $performer_id}]) {
                    id
                    visual_files {
                    ... on ImageFile {
                        id
                        path
                    }
                    ... on VideoFile {
                        id
                        path
                    }
                    }
                    performers {
                    id
                    name
                    }
                }
            }
        """)
        self.studioCreate_by_name = gql("""
            mutation studioCreate($studio_name: String!) {
            studioCreate(input: {name: $studio_name}) {
                id
                name
            }
            }
          """)
        self.performerCreate_by_name = gql("""
            mutation performerCreate($perf_name: String!) {
            performerCreate(input: {name: $perf_name}) {
                id
                name
            }
            }
          """)
        self.my_mutations = {
            "sceneUpdate_by_tag_id" : self.sceneUpdate_by_tag_id,
            "sceneUpdate_rating" : self.sceneUpdate_rating,
            "sceneUpdate_multiple": self.sceneUpdate_multiple,
            "imageUpdate_with_performer": self.imageUpdate_with_performer,
            "studioCreate_by_name": self.studioCreate_by_name,
            "performerCreate_by_name": self.performerCreate_by_name
        }
        
    def send_query(self, query, var_dict):
        return self.client.execute(query, variable_values=var_dict)
    
    def testfunction(self):
      print(self.send_query(query=self.my_queries["findTags_by_name"], var_dict={"tag_name": "to_img"}))
    
    def update_scene_tags(self, scene, tags_to_add=[], tags_to_delete=[]):
      """
      Parse existing scene tags and update
      @param scene scene: json response from query findScenes
      @param tags_to_add: list of tag_ids to add to the scene
      @param tags_to_delete: list of tags to delete from the scene
      """
      tag_id_str = "["
      tag_id_list = []
      
      for idx, tag_data in enumerate(scene["tags"]):
          if int(tag_data["id"]) not in tags_to_delete:
            tag_id_list.append(int(tag_data["id"]))

      for tag in tags_to_add:
        tag_id_list.append(tag)

      return self.send_query(query=self.my_mutations["sceneUpdate_by_tag_id"], 
                                          var_dict={"scene_id": scene["id"], "tag_id_str": tag_id_list})
    
    def parse_tags_to_int_list(self, scene):
        """
        Helper method to parse tags to an integer list.
        @param scene: json response from query findScenes of Scene type
        """
        tag_id_list = []
        for tag in scene["tags"]:
            tag_id_list.append(int(tag["id"]))
        return tag_id_list
    
    def find_helper_tag_ids_by_name(self):
        tag_name_list = "to_delete", "deleted", "to_img", "processing"
        for tag_name in tag_name_list:
            print(self.send_query(query=self.my_queries["findTags_by_name"], var_dict={"tag_name": tag_name}))
    
    def remove_tag_from_all(self, tag_id):
        """
        Helper method to remove tag_id from all scenes in database.
        """
        processing_found = True
        while processing_found:
            processing_scenes = self.send_query(query=self.my_queries["findScenes_by_tag_id"], 
                                var_dict={"tag_id" : tag_id})
            if processing_scenes["findScenes"]["scenes"] != []:
                for scene in processing_scenes["findScenes"]["scenes"]:
                    resp = self.update_scene_tags(scene=scene, tags_to_delete=[tag_id])
            else:
                processing_found = False
                print("Removed {} helper tag from all files.".format(self.tag_name[tag_id]))
    
    def to_title(self, name):
        title = name.replace("_", " ")
        title = name.title()
        return title
        
    def vid_to_img(self):
        """
        
        WORKING
        
        TODO: code cleanup and comments
            
        python3 ./stash_server_updater.py --to_img
        
        Parse to_img tag to move short OF clips to image clips. This method will
        keep vid phash in vid directory so if files are re-added they can be picked up
        by delete_files_keep_stash_entry(). 
        
        Corner cases not covered: 
        - multiple performers
        - Only for OF rn
        
        Known bugs
        - If to_img processing loop fails due to bad gui workflow, performers won't be added. Need to add try catch.
        """

        to_img_found = True
        processing_found = True
        file_count = 0
        
        delete_after_scan = []
        fields = ["filename", "performer"]
        to_img_fields_base = {
            "path": None,
            "performer": None,
            "performer_id": None
        }
        to_img_list = []
        
        while to_img_found:           
            to_img_scenes = self.send_query(query=self.my_queries["findScenes_by_tag_id"], 
                                        var_dict={"tag_id" : self.tag_id["TO_IMG"]})

            if to_img_scenes["findScenes"]["scenes"] != []:
                for scene in to_img_scenes["findScenes"]["scenes"]:
                    resp = self.update_scene_tags(scene=scene, tags_to_add=[self.tag_id["DELETED"]], 
                                                  tags_to_delete=[self.tag_id["TO_IMG"]])
                    # Copy to cleanup to allow for vid to be removed to prevent later re-add
                    # in vid library. Cannot just move directly to img dir
                    path = scene["files"][0]["path"]
                    print(path)
                    filename = os.path.basename(path)
                    shutil.copy(path, self.cleanup_dir)
                    delete_after_scan.append(self.cleanup_dir + "/" + filename)
                    #Move to img directory of same performer
                    perf = scene["performers"][0]["name"]
                    scene_output_dir = os.path.join(self.creator_dir, perf)
                    scene_output_file = scene_output_dir + "/" + filename
                    
                    # save scene output file (img dir) + performer to readd after parse
                    to_img_fields = copy.deepcopy(to_img_fields_base)
                    to_img_fields["path"] = scene_output_file
                    to_img_fields["performer"] = perf
                    to_img_fields["performer_id"] = scene["performers"][0]["id"]
                    to_img_list.append(to_img_fields)
                    # make img dir if it doesn't exist
                    os.makedirs(scene_output_dir, exist_ok=True)
                    shutil.move(path, scene_output_file)
                    file_count += 1
            else:
                #self.remove_tag_from_all(tag_id=self.tag_id["PROCESSING"])
                to_img_found = False
                input("Run Tasks > Library > Scan to pick up new files in cleanup and image directories.")
                # Delete files in cleanup dir now that they are re-added to vid lib
                for file in delete_after_scan:
                    os.remove(file)
                print("{} new files deleted".format(file_count))
                for img_clip in to_img_list:
                    img_clip_resp = self.send_query(query=self.my_queries["findImages_by_path"], 
                                        var_dict={"path" : img_clip["path"]})
                    resp = self.send_query(query=self.my_mutations["imageUpdate_with_performer"], 
                                          var_dict={"image_id": img_clip_resp["findImages"]["images"][0]["id"], 
                                                    "performer_id": [img_clip["performer_id"]]})

    def delete_files_keep_stash_entry(self):
        """
        WORKING
        
        python3 ./stash_server_updater.py --to_delete
        Deletes files with to_delete tag and replaces tag with deleted.
        Keeps rest of tags. Must have luks open and stash running
        """
        to_delete_found = True
        deleted_found = True
        processing_found = True
        file_count = 0
        print("Checking for files with to_delete flag: ")
        # Query only returns 25 at a time so have to loop
        while to_delete_found:
            to_delete_scenes = self.send_query(query=self.my_queries["findScenes_by_tag_id"], 
                                        var_dict={"tag_id" : self.tag_id["TO_DELETE"]})
            if to_delete_scenes["findScenes"]["scenes"] != []:
                for scene in to_delete_scenes["findScenes"]["scenes"]:
                    self.update_scene_tags(scene=scene, tags_to_add=[self.tag_id["DELETED"]], 
                                           tags_to_delete=[self.tag_id["TO_DELETE"]])
                    # delete file
                    print(scene["files"][0]["path"])
                    file_count += 1
                    try:
                        os.remove(scene["files"][0]["path"])
                    except:
                        print("^^^File not found!")
                    else:
            else:
                to_delete_found = False
                print("{} new files deleted".format(file_count))
                
        file_count = 0
        print("Checking for files previously deleted and re-added:")
        while deleted_found:
            deleted_not_processing = self.send_query(query=self.my_queries["findScenes_by_tag_and_tagex"], 
                                        var_dict={"tag_id" : self.tag_id["DELETED"], 
                                                  "tag_id_ex": self.tag_id["PROCESSING"]})
            if deleted_not_processing["findScenes"]["scenes"] != []:
                for scene in deleted_not_processing["findScenes"]["scenes"]:
                    resp = self.update_scene_tags(scene=scene, tags_to_add=[self.tag_id["PROCESSING"]])
                    if os.path.exists(scene["files"][0]["path"]):
                        print(scene["files"][0]["path"])
                        file_count += 1
                        os.remove(scene["files"][0]["path"])
            else:
                deleted_found = False
                print("{} re-added files deleted".format(file_count))
                self.remove_tag_from_all(tag_id=self.tag_id["PROCESSING"])

        
    def filename_parser(self):
        """
        todo
        
        Algo
        Add folder to stash dir in correct location if not already there 
        Use BRU (bulk rename utility. runs fine with Wine on linux) 
        to make sure files are in the following format:
        <studio/creator>-<scene_name>-<performer1>,<performer2>.<ext>
        
        Iterate through files in target dir
        
        check if studio or creator exists, if it doesn't show a pop up to create it
        rename scene to scene_name (default No Title)
        check performer list and show pop ups on performer creation and assign performer
        """
        
        filename_re_obj = re.compile("([^-]+)-([^-]+)-?([^-]*)\.")
        confirm_performer_creation = False

        for filename in os.listdir(self.filename_parser_target_dir):
            path = self.filename_parser_target_dir + "/" + filename
            re_match = filename_re_obj.findall(filename)
            studio, title, performers = re_match[0][0], re_match[0][1], re_match[0][2]
            if performers == "":
                performers = None
                performers_list = None
            if not re_match:
                print("regex match not found on filename, check naming conventions and rename. File: {}".format(self.filename_parser_target_dir + filename))
            scene_query = self.send_query(query=self.my_queries["findScenes_by_path"], 
                                        var_dict={"path": path})
            if len(scene_query["findScenes"]["scenes"]) == 0:
                raise Exception("Scene at path {} not found. Run a library scan to pickup recently added files.".format(path))
            else:
                scene_id = scene_query["findScenes"]["scenes"][0]["id"]
            # Get studio
            studio_query = self.send_query(query=self.my_queries["findStudios_by_name"], 
                                        var_dict={"studio_name": studio})
            if len(studio_query["findStudios"]["studios"]) == 0:
                print(path)
                studio_title = self.to_title(studio)
                input("Studio \"{}\" not found. Hit ENTER to add.".format(studio_title))
                # Create studio
                resp = self.send_query(query=self.my_mutations["studioCreate_by_name"], 
                                          var_dict={"studio_name": studio})
                studio_id = resp["studioCreate"]["id"]
            else:
                studio_id = studio_query["findStudios"]["studios"][0]["id"]
            # Get Title
            if "notitle" in title:
                title = "No Title"
            else:
                title = title.replace("_", " ")
                title = title.title()
                
            # Add Performer(s)
            
            perf_id_list = []
            if performers != None:
                performers_list = performers.split(",")
                for performer in performers_list: 
                    performer = performer.replace("_", " ")
                    performer = performer.title()
                    perf_query = self.send_query(query=self.my_queries["findPerformers_by_name"], 
                                            var_dict={"perf_name": performer})
                    if len(perf_query["findPerformers"]["performers"]) == 0:
                        if confirm_performer_creation:
                            input("Perfomer {} not found. Press ENTER to create.".format(performer))
                        resp = self.send_query(query=self.my_mutations["performerCreate_by_name"], 
                                            var_dict={"perf_name": performer})
                        perf_id = resp["performerCreate"]["id"]
                    else:
                        perf_id = perf_query["findPerformers"]["performers"][0]["id"]
                    perf_id_list.append(perf_id)

            print("Debug: {}, {}, {}, {}".format(path, studio, title, performers_list))
            # can have mutations and dont fill all vars, can fill to None if wanted and rest will go through
            resp = self.send_query(query=self.my_mutations["sceneUpdate_multiple"], 
                                          var_dict={"scene_id": scene_id, "studio_id": studio_id, "title": title, 
                                                    "performer_ids": perf_id_list})

    def stash_vr_issue_86_parser(self):
        """
        custom parser for https://github.com/o-fl0w/stash-vr/issues/86
        """
        query_hit = True
        filename_re_obj = re.compile("\d\d\d\d-\d\d-\d\d")
        bad_scene_list = []
        print("Checking for files with invalid date format: ")
        # Query only returns 25 at a time so have to loop
        while query_hit:
            scene_resp = self.send_query(query=self.my_queries["findScenes_by_date_valid"], 
                                        var_dict={"date": "0000-00-00", 
                                                  "tag_id_ex": self.tag_id["PROCESSING"]})
            if scene_resp["findScenes"]["scenes"] != []:
                for scene in scene_resp["findScenes"]["scenes"]:
                    resp = self.update_scene_tags(scene=scene, tags_to_add=[self.tag_id["PROCESSING"]])
                    re_match = filename_re_obj.findall(scene["date"])
                    
                    if re_match:
                        print("Good date: {}".format(scene["date"]))
                    else:
                        print("Bad scene found: file {}, date: {}".format(scene["files"][0]["path"], scene["date"]))
                        bad_scene_list.append((scene["files"][0]["path"], scene["date"]))
            else:
                query_hit = False
                self.remove_tag_from_all(tag_id=self.tag_id["PROCESSING"])
        
        if len(bad_scene_list) != 0:
            print("{} bad dates found".format(len(bad_scene_list)))
            for scene in bad_scene_list:
                print(scene)
        else:
            print("no bad scenes found!")
            
def main():
    stash_utils = StashUtils()
    if stash_utils.args.to_img:
        stash_utils.vid_to_img()
    elif stash_utils.args.to_delete:
        stash_utils.delete_files_keep_stash_entry()
    elif stash_utils.args.filename_parser:
        stash_utils.filename_parser()
    elif stash_utils.args.get_helper_tag_ids:
        stash_utils.find_helper_tag_ids_by_name()
    elif stash_utils.args.issue_86:
        stash_utils.stash_vr_issue_86_parser()
    elif stash_utils.args.test:
        stash_utils.testfunction()
    else:
        print("Function not found! Check spelling.")
        
if __name__=="__main__":
    main()
